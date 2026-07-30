#!/usr/bin/env python3
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "confluent-kafka",
#     "fastavro",
#     "lsst-alert-packet",
#     "numpy",
#     "pandas",
# ]
# ///
"""
Extract LSST simulated pipeline results from Kafka into CSV files.

Outputs:
  photometry.csv  – one row per detection for every object that passed the
                    filter. 'passed_filter' is True for the detection that
                    triggered the pass.
  stats.csv       – one row per passing object with aggregate statistics,
                    including filter efficiency (passed / total input objects).

Usage:
    python extract_output.py [--broker localhost:9092]
    uv run tests/lsst-simulated/extract_output.py --data-dir data_01_07
"""

import argparse
import collections
import csv
import io
import math
import uuid
from pathlib import Path

import fastavro
import lsst.alert.packet as packet
import pandas as pd
from confluent_kafka import Consumer

LSST_ZP_AB_NJY = 31.4
FACTOR = 2.5 / math.log(10)

PHOTOMETRY_COLS = [
    "object_id",
    "candid",
    "jd",
    "mjd",
    "band",
    "flux_njy",
    "flux_err_njy",
    "mag",
    "mag_err",
    "passed_filter",
]

STATS_COLS = [
    "object_id",
    "target_name",
    "ra",
    "dec",
    "n_detections",
    "n_passed_filter",
    "filter_names",
    "first_jd",
    "last_jd",
]


def flux_to_mag(flux: float) -> float | None:
    if flux is None or flux <= 0:
        return None
    return -2.5 * math.log10(flux) + LSST_ZP_AB_NJY


def flux_err_to_mag_err(flux: float, flux_err: float) -> float:
    return FACTOR * flux_err / flux


def jd_to_mjd(jd: float) -> float:
    return jd - 2400000.5


def read_passing_alerts(broker: str) -> dict[str, list[dict]]:
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": f"extract-output-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["LSST_alerts_results"])

    alerts_by_object: dict[str, list[dict]] = collections.defaultdict(list)
    while True:
        msg = consumer.poll(5.0)
        if msg is None:
            break
        if msg.error():
            break
        reader = fastavro.reader(io.BytesIO(msg.value()))
        for record in reader:
            alerts_by_object[record["objectId"]].append(record)

    consumer.close()
    return alerts_by_object


def count_input_objects(broker: str, schema_version: str = "11.0") -> int:
    """Count distinct diaObjectId values in the alerts-simulated topic."""
    sr = packet.SchemaRegistry.from_filesystem()
    schema = sr.get_by_version(schema_version)

    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": f"extract-input-count-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["alerts-simulated"])

    object_ids: set[int] = set()
    while True:
        msg = consumer.poll(5.0)
        if msg is None:
            break
        if msg.error():
            break
        payload = msg.value()
        # Strip 5-byte Confluent SR header: [0x00][4B schema_id]
        avro_bytes = payload[5:]
        alert = schema.deserialize(avro_bytes)
        object_ids.add(alert["diaSource"]["diaObjectId"])

    consumer.close()
    return len(object_ids)


def build_target_lookup(data_dir: Path) -> dict[int, str]:
    """Return a mapping objectid → target_name using the data directory CSVs."""
    summary = pd.read_csv(data_dir / "summary.csv")
    opsim = pd.read_csv(
        data_dir / "opsim_fields_index.csv", usecols=["field_index", "target_name"]
    )
    merged = summary.merge(opsim, left_on="field_idx", right_on="field_index")

    lookup: dict[int, str] = {}
    for csv_file in sorted(data_dir.glob("lightcurve_LSSTlike_*.csv")):
        lc_index = int(csv_file.stem.split("_")[-1])
        row = merged[merged["i"] == lc_index]
        if row.empty:
            continue
        try:
            first_row = pd.read_csv(csv_file, usecols=["objectid"], nrows=1)
            object_id = int(first_row["objectid"].iloc[0])
            if object_id == 0:
                object_id = lc_index + 1_000_000
            lookup[object_id] = str(row["target_name"].iloc[0])
        except Exception:
            continue
    return lookup


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--photometry-out", default="photometry.csv")
    parser.add_argument("--stats-out", default="stats.csv")
    parser.add_argument(
        "--data-dir", default=None, help="Path to data directory for target_name lookup"
    )
    args = parser.parse_args()

    target_lookup: dict[int, str] = {}
    if args.data_dir:
        print(f"Building target name lookup from {args.data_dir}...")
        target_lookup = build_target_lookup(Path(args.data_dir))
        print(f"  Loaded {len(target_lookup)} object→target mappings")

    print("Reading filter results from Kafka...")
    alerts_by_object = read_passing_alerts(args.broker)
    n_passing = len(alerts_by_object)

    print("Counting total input objects from alerts-simulated topic...")
    n_total = count_input_objects(args.broker)

    pct = 100.0 * n_passing / n_total if n_total > 0 else 0.0
    print(f"\nFilter efficiency: {n_passing} / {n_total} objects passed ({pct:.1f}%)\n")

    with (
        open(args.photometry_out, "w", newline="") as phot_f,
        open(args.stats_out, "w", newline="") as stats_f,
    ):
        phot_w = csv.DictWriter(phot_f, fieldnames=PHOTOMETRY_COLS)
        phot_w.writeheader()
        stats_w = csv.DictWriter(stats_f, fieldnames=STATS_COLS)
        stats_w.writeheader()

        for object_id, msgs in sorted(alerts_by_object.items()):
            # Map jd → candid for every detection that triggered a filter pass
            passing: dict[float, int] = {m["jd"]: m["candid"] for m in msgs}

            # Use the message with the most photometry points (latest alert)
            best = max(msgs, key=lambda m: len(m["photometry"]))

            filter_names = sorted(
                {f["filter_name"] for m in msgs for f in m["filters"]}
            )

            detections = [p for p in best["photometry"] if p["origin"] == "Alert"]
            detections.sort(key=lambda p: p["jd"])

            first_jd = detections[0]["jd"] if detections else None
            last_jd = detections[-1]["jd"] if detections else None

            for point in detections:
                jd = point["jd"]
                flux = point["flux"]
                flux_err = point["flux_err"]
                mag = flux_to_mag(flux)
                mag_err = flux_err_to_mag_err(flux, flux_err) if flux else None

                phot_w.writerow(
                    {
                        "object_id": object_id,
                        "candid": passing.get(jd, ""),
                        "jd": jd,
                        "mjd": f"{jd_to_mjd(jd):.6f}",
                        "band": point["band"],
                        "flux_njy": flux if flux is not None else "",
                        "flux_err_njy": flux_err,
                        "mag": f"{mag:.4f}" if mag is not None else "",
                        "mag_err": f"{mag_err:.4f}" if mag_err is not None else "",
                        "passed_filter": jd in passing,
                    }
                )

            stats_w.writerow(
                {
                    "object_id": object_id,
                    "target_name": target_lookup.get(int(object_id), ""),
                    "ra": best["ra"],
                    "dec": best["dec"],
                    "n_detections": len(detections),
                    "n_passed_filter": len(passing),
                    "filter_names": "|".join(filter_names),
                    "first_jd": first_jd,
                    "last_jd": last_jd,
                }
            )

    print(f"Wrote {args.photometry_out}")
    print(f"Wrote {args.stats_out}")


if __name__ == "__main__":
    main()
