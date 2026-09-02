#!/usr/bin/env python3
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "confluent-kafka",
#     "fastavro",
#     "numpy",
#     "pandas",
#     "pyyaml",
# ]
# ///
"""
Extract LSST simulated pipeline results from Kafka into CSV files.

Outputs written to results/<sim-id>/:
  photometry.csv  – one row per passing alert (one per detection that triggered
                    the filter). Columns: object_id, candid, jd, mjd, band,
                    flux_njy, flux_err_njy, mag, mag_err.
  stats.csv       – one row per passing object with aggregate statistics
                    (n_detections from raw CSV, n_passed_filter from Kafka,
                    filter efficiency).

Usage:
    uv run tests/lsst-simulated/extract_output.py --config tests/lsst-simulated/example-config.yaml --sim-id cdo_tai_20260902_151041
    uv run tests/lsst-simulated/extract_output.py --data-dir /path/to/dataset --sim-id cdo_20260819_143022
"""

import argparse
import collections
import csv
import io
import math
import uuid
from pathlib import Path

import fastavro
import pandas as pd
import yaml
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


def resolve_csv_files(data_dir: Path, ids=None, obj_range=None, limit: int = 0) -> list[Path]:
    all_csv = sorted(
        data_dir.glob("lightcurve_*.csv"), key=lambda f: int(f.stem.split("_")[-1])
    )
    if ids is not None:
        id_set = set(ids)
        return [f for f in all_csv if int(f.stem.split("_")[-1]) in id_set]
    if obj_range is not None:
        start, end = obj_range[0], obj_range[1]
        return [f for f in all_csv if start <= int(f.stem.split("_")[-1]) <= end]
    return all_csv[:limit] if limit > 0 else all_csv


def read_passing_alerts(broker: str, sim_id: str | None) -> dict[str, list[dict]]:
    topic = f"LSST_alerts_results_{sim_id}" if sim_id else "LSST_alerts_results"
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": f"extract-output-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])

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


def build_detection_count_lookup(csv_files: list[Path]) -> dict[str, int]:
    """Return {object_id_str: n_detections} from raw lightcurve CSV files."""
    lookup: dict[str, int] = {}
    for f in csv_files:
        lc_index = int(f.stem.split("_")[-1])
        obj_id = lc_index if lc_index != 0 else 1_000_000
        lc = pd.read_csv(f, usecols=["time_mjd"])
        lookup[str(obj_id)] = len(lc)
    return lookup


def build_candid_lookup(csv_files: list[Path]) -> dict[tuple[str, float], int]:
    """Return {(object_id_str, jd): candid} from the local lightcurve CSV files."""
    lookup: dict[tuple[str, float], int] = {}
    for f in csv_files:
        lc_index = int(f.stem.split("_")[-1])
        obj_id = lc_index if lc_index != 0 else 1_000_000
        lc = pd.read_csv(f, usecols=["time_mjd", "candidate_id"])
        for _, row in lc.iterrows():
            seq = int(str(row["candidate_id"]).split("_", 1)[1])
            candid = obj_id * 100_000 + seq
            jd = float(row["time_mjd"]) + 2400000.5
            lookup[(str(obj_id), jd)] = candid
    return lookup


def build_target_lookup(data_dir: Path) -> dict[int, str]:
    """Return a mapping objectid → target_name using the data directory CSVs."""
    summary = pd.read_csv(data_dir / "summary.csv")

    if "target_name" in summary.columns:
        lookup: dict[int, str] = {}
        for csv_file in sorted(data_dir.glob("lightcurve_*.csv")):
            lc_index = int(csv_file.stem.split("_")[-1])
            row = summary[summary["i"] == lc_index]
            if row.empty:
                continue
            object_id = lc_index if lc_index != 0 else lc_index + 1_000_000
            lookup[object_id] = str(row["target_name"].iloc[0])
        return lookup

    opsim = pd.read_csv(
        data_dir / "opsim_fields_index.csv", usecols=["field_index", "target_name"]
    )
    merged = summary.merge(opsim, left_on="field_idx", right_on="field_index")
    lookup = {}
    for csv_file in sorted(data_dir.glob("lightcurve_*.csv")):
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
    parser.add_argument("--broker", default=None, help="Kafka broker address (overrides config)")
    parser.add_argument(
        "--config", default=None, help="Path to simulation config YAML file"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for output files (default: results/<sim-id> or results/default)",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Path to data directory (overrides config)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max objects to consider (0 = no limit); ignored when config specifies ids or range",
    )
    parser.add_argument(
        "--sim-id",
        default=None,
        help="Simulation ID (e.g. cdo_tai_20260902_151041). Reads from the sim-scoped Kafka topics.",
    )
    args = parser.parse_args()

    cfg: dict = {}
    if args.config:
        with open(args.config) as f:
            cfg = yaml.safe_load(f) or {}

    broker = args.broker or cfg.get("broker", "localhost:9092")
    data_dir_str = args.data_dir or cfg.get("data_dir")
    ids = cfg.get("ids")
    obj_range = cfg.get("range")

    sim_id: str | None = args.sim_id
    out_dir = Path(args.output_dir) if args.output_dir else Path("results") / (sim_id or "default")
    out_dir.mkdir(parents=True, exist_ok=True)
    photometry_out = out_dir / "photometry.csv"
    stats_out = out_dir / "stats.csv"

    if sim_id:
        print(f"Simulation ID : {sim_id}")
    print(f"Output dir    : {out_dir}/")

    target_lookup: dict[int, str] = {}
    detection_count_lookup: dict[str, int] = {}
    csv_files: list[Path] = []

    if data_dir_str:
        data_dir = Path(data_dir_str)
        csv_files = resolve_csv_files(data_dir, ids=ids, obj_range=obj_range, limit=args.limit)
        print(f"Building target name lookup from {data_dir_str} ({len(csv_files)} objects)...")
        target_lookup = build_target_lookup(data_dir)
        print(f"  Loaded {len(target_lookup)} object→target mappings")
        detection_count_lookup = build_detection_count_lookup(csv_files)

    print("Reading filter results from Kafka...")
    alerts_by_object = read_passing_alerts(broker, sim_id)
    n_passing = len(alerts_by_object)

    n_total = len(csv_files) if csv_files else 0
    if n_total:
        print(f"Total input objects: {n_total} (from data directory)")
    else:
        print("Total input objects: unknown (pass --data-dir or --config to count)")

    pct = 100.0 * n_passing / n_total if n_total > 0 else 0.0
    print(f"\nFilter efficiency: {n_passing} / {n_total} objects passed ({pct:.1f}%)\n")

    with (
        open(photometry_out, "w", newline="") as phot_f,
        open(stats_out, "w", newline="") as stats_f,
    ):
        phot_w = csv.DictWriter(phot_f, fieldnames=PHOTOMETRY_COLS)
        phot_w.writeheader()
        stats_w = csv.DictWriter(stats_f, fieldnames=STATS_COLS)
        stats_w.writeheader()

        for object_id, msgs in sorted(alerts_by_object.items(), key=lambda x: int(x[0])):
            msgs_sorted = sorted(msgs, key=lambda m: m["candid"])

            filter_names = sorted(
                {f["filter_name"] for m in msgs for f in m["filters"]}
            )

            first_jd = msgs_sorted[0]["jd"]
            last_jd = msgs_sorted[-1]["jd"]

            for m in msgs_sorted:
                jd = m["jd"]
                current = next(
                    (p for p in m["photometry"] if p["origin"] == "Alert" and p["jd"] == jd),
                    None,
                )
                flux = current["flux"] if current else None
                flux_err = current["flux_err"] if current else None
                band = current["band"] if current else ""
                mag = flux_to_mag(flux)
                mag_err = flux_err_to_mag_err(flux, flux_err) if flux else None

                phot_w.writerow(
                    {
                        "object_id": object_id,
                        "candid": m["candid"],
                        "jd": jd,
                        "mjd": f"{jd_to_mjd(jd):.6f}",
                        "band": band,
                        "flux_njy": flux if flux is not None else "",
                        "flux_err_njy": flux_err if flux_err is not None else "",
                        "mag": f"{mag:.4f}" if mag is not None else "",
                        "mag_err": f"{mag_err:.4f}" if mag_err is not None else "",
                    }
                )

            best = max(msgs, key=lambda m: len(m["photometry"]))
            stats_w.writerow(
                {
                    "object_id": object_id,
                    "target_name": target_lookup.get(int(object_id), ""),
                    "ra": best["ra"],
                    "dec": best["dec"],
                    "n_detections": detection_count_lookup.get(object_id, len(msgs_sorted)),
                    "n_passed_filter": len(msgs_sorted),
                    "filter_names": "|".join(filter_names),
                    "first_jd": first_jd,
                    "last_jd": last_jd,
                }
            )

    print(f"Wrote {photometry_out}")
    print(f"Wrote {stats_out}")


if __name__ == "__main__":
    main()
