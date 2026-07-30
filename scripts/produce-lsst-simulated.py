#!/usr/bin/env python3
"""
Produce simulated LSST-like alerts from CSV files to Kafka topic 'alerts-simulated'.

Each CSV file in data_alex/ is treated as one simulated source (object).
Only rows where mag_true is finite are used:
  - detected=True  → diaSource (detection), contributes to prvDiaSources in subsequent alerts
  - detected=False → prvDiaForcedSource (non-detection / upper limit)

One Kafka message is produced per detection, carrying the full history up to that point.

Avro format: Confluent Schema Registry format
  [0x00][4-byte schema_id big-endian][avro datum]
  schema_id = 1100  (LSST v11.0)

Usage:
    .venv/bin/python scripts/produce-lsst-simulated.py [--broker localhost:9092] [--limit N] [--reset]
"""

import argparse
import math
import multiprocessing as mp
import struct
import time
from pathlib import Path

import lsst.alert.packet as packet
import numpy as np
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

SCHEMA_VERSION = "11.0"
SCHEMA_ID = 1100  # boom maps 1100 → v11.0 via fallback
SR_HEADER = b"\x00" + struct.pack(">I", SCHEMA_ID)  # Confluent Schema Registry prefix

KAFKA_TOPIC = "alerts-simulated"
DATA_DIR = Path(__file__).parent.parent / "data_alex"

# LSST photometric zero-point in nJy: ZP = 8.9 (AB) + 22.5 (nJy correction)
ZP_NJY = 31.4
# Conversion factor used in boom: 2.5 / ln(10)
FACTOR = 2.5 / math.log(10)

# ---------------------------------------------------------------------------
# Magnitude / flux conversions (mirrors boom's lightcurves.rs)
# ---------------------------------------------------------------------------


def mag_to_flux(mag: float, zp: float = ZP_NJY) -> float:
    """AB magnitude → flux in nJy."""
    return 10.0 ** (-0.4 * (mag - zp))


def mag_err_to_flux_err(mag_err: float, flux: float) -> float:
    """Magnitude error → flux error (linear approximation)."""
    return flux * mag_err / FACTOR


def mag_ulim_to_flux_err(mag_ulim: float, zp: float = ZP_NJY) -> float:
    """5-sigma limiting magnitude → 1-sigma flux error.

    Inverts: mag_ulim = -2.5 * log10(5 * flux_err) + zp
    """
    return mag_to_flux(mag_ulim, zp) / 5.0


# ---------------------------------------------------------------------------
# Alert record builders
# ---------------------------------------------------------------------------


def _dia_source(
    candid: int,
    object_id: int,
    ra: float,
    dec: float,
    mjd: float,
    band: str,
    flux: float,
    flux_err: float,
    snr: float | None,
    visit: int,
    detector: int = 0,
) -> dict:
    """Build a diaSource dict from photometric values."""
    return {
        "diaSourceId": candid,
        "diaObjectId": object_id,
        "timeProcessedMjdTai": mjd,
        "visit": visit,
        "detector": detector,
        "midpointMjdTai": mjd,
        "band": band,
        "programId": 1,
        "ra": ra,
        "dec": dec,
        "raErr": 1e-4,
        "decErr": 1e-4,
        "ra_dec_Cov": 0.0,
        "x": 0.0,
        "y": 0.0,
        "xErr": 1.0,
        "yErr": 1.0,
        "x_y_Cov": 0.0,
        "psfFlux": flux,
        "psfFluxErr": flux_err,
        "apFlux": flux,
        "apFluxErr": flux_err,
        "snr": snr,
        "psfNdata": 25,
        "trailNdata": 0,
        "dipoleNdata": 0,
        "bboxSize": 50,
        # Quality flags — default to False (no issues) for simulated data
        "shape_flag": False,
        "apFlux_flag": False,
        "apFlux_flag_aperture_used": False,
        "centroid_flag": False,
        "isDipole": False,
        "forced_PsfFlux_flag": False,
        "forced_PsfFlux_flag_edge": False,
        "psfFlux_flag": False,
        "pixelFlags_bad": False,
        "pixelFlags_cr": False,
        "pixelFlags_crCenter": False,
        "pixelFlags_edge": False,
        "pixelFlags_interpolated": False,
        "pixelFlags_interpolatedCenter": False,
        "pixelFlags_saturated": False,
        "pixelFlags_saturatedCenter": False,
        "pixelFlags_streak": False,
        "pixelFlags_streakCenter": False,
        "pixelFlags_suspect": False,
        "pixelFlags_suspectCenter": False,
    }


def _dia_forced_source(
    forced_id: int,
    object_id: int,
    ra: float,
    dec: float,
    mjd: float,
    band: str,
    flux_err: float,
    visit: int,
    detector: int = 0,
) -> dict:
    """Build a diaForcedSource dict from a non-detection upper limit."""
    return {
        "diaForcedSourceId": forced_id,
        "diaObjectId": object_id,
        "ra": ra,
        "dec": dec,
        "visit": visit,
        "detector": detector,
        "midpointMjdTai": mjd,
        "timeProcessedMjdTai": mjd,
        "psfFlux": 0.0,
        "psfFluxErr": flux_err,
        "band": band,
    }


def _dia_object(
    object_id: int,
    ra: float,
    dec: float,
    first_mjd: float,
    last_mjd: float,
    n_sources: int,
) -> dict:
    """Build a minimal diaObject dict.

    All non-nullable fields required by the v11.0 schema must be present,
    including the per-band psfFluxNdata counts (set to 0 since we don't track
    them per band here).
    """
    return {
        "diaObjectId": object_id,
        "validityStartMjdTai": last_mjd,
        "ra": ra,
        "dec": dec,
        "raErr": 1e-4,
        "decErr": 1e-4,
        "ra_dec_Cov": 0.0,
        "firstDiaSourceMjdTai": first_mjd,
        "lastDiaSourceMjdTai": last_mjd,
        "nDiaSources": n_sources,
        # Required non-nullable band detection counts
        "u_psfFluxNdata": 0,
        "g_psfFluxNdata": 0,
        "r_psfFluxNdata": 0,
        "i_psfFluxNdata": 0,
        "z_psfFluxNdata": 0,
        "y_psfFluxNdata": 0,
    }


_EMPTY_CUTOUT = b"\x00" * 16  # placeholder — boom requires non-null cutout bytes


def _alert(
    candid: int,
    dia_source: dict,
    prv_dia_sources: list[dict],
    prv_dia_forced_sources: list[dict],
    dia_object: dict | None,
) -> dict:
    """Assemble the top-level alert dict."""
    return {
        "diaSourceId": candid,
        "diaSource": dia_source,
        "prvDiaSources": prv_dia_sources if prv_dia_sources else None,
        "prvDiaForcedSources": prv_dia_forced_sources
        if prv_dia_forced_sources
        else None,
        "diaObject": dia_object,
        "cutoutDifference": _EMPTY_CUTOUT,
        "cutoutScience": _EMPTY_CUTOUT,
        "cutoutTemplate": _EMPTY_CUTOUT,
    }


# ---------------------------------------------------------------------------
# CSV → alerts
# ---------------------------------------------------------------------------


def _load_position_lookup(data_dir: Path) -> dict[int, tuple[float, float]]:
    """Build a mapping from lightcurve file index → (ra_deg, dec_deg).

    The filename index (e.g. 0009 from lightcurve_LSSTlike_0009.csv) matches
    column `i` in summary.csv.  That row's `field_idx` maps to `field_index`
    in opsim_fields_index.csv where the true RA/Dec are stored.
    """
    summary = pd.read_csv(data_dir / "summary.csv")
    opsim = pd.read_csv(data_dir / "opsim_fields_index.csv")
    merged = summary.merge(opsim, left_on="field_idx", right_on="field_index")
    return {
        int(row["i"]): (float(row["ra0_deg"]), float(row["dec0_deg"]))
        for _, row in merged.iterrows()
    }


def alerts_from_csv(
    filepath: Path, lc_index: int, ra: float, dec: float
) -> list[dict]:
    """
    Read a CSV file and generate one alert per detection.

    Filtering rule: only rows where mag_true is finite are kept.
    - detected=True  → detection alert (diaSource)
    - detected=False → non-detection (prvDiaForcedSource in future alerts)

    object_id and candid are read directly from the 'objectid' and
    'candidate_id' CSV columns (e.g. candidate_id="51615_3" → candid derived
    as objectid * 100_000 + sequence).  lc_index is kept only for the
    forced-source ID fallback.
    """
    df = pd.read_csv(filepath)

    # Keep only rows where mag_true is a real finite number
    df = df[df["mag_true"].notna() & np.isfinite(df["mag_true"])].copy()
    df = df.sort_values("observationStartMJD").reset_index(drop=True)

    if df.empty:
        return []

    # Read object_id from the CSV column; boom rejects diaObjectId=0 as missing
    object_id = int(df["objectid"].iloc[0])
    if object_id == 0:
        object_id = lc_index + 1_000_000

    detections: list[dict] = []  # accumulates diaSource records
    non_detections: list[dict] = []  # accumulates diaForcedSource records
    alerts: list[dict] = []

    detection_count = 0

    for visit_idx, row in df.iterrows():
        detected = str(row.get("detected", "False")).strip().lower() == "true"
        mjd = float(row["observationStartMJD"])
        band = str(row["band"])

        if detected:
            if pd.isna(row.get("mag_obs")):
                continue  # skip if no magnitude despite detected=True

            flux = mag_to_flux(float(row["mag_obs"]))
            mag_err = row.get("mag_err")
            flux_err = (
                mag_err_to_flux_err(float(mag_err), flux)
                if pd.notna(mag_err)
                else flux * 0.05
            )
            snr_val = row.get("snr_obs")
            snr = float(snr_val) if pd.notna(snr_val) else None

            # Derive integer candid from candidate_id column (e.g. "51615_3" → seq=3)
            cid_str = str(row["candidate_id"])
            seq = int(cid_str.split("_", 1)[1])
            candid = object_id * 100_000 + seq
            detection_count += 1

            dia_src = _dia_source(
                candid=candid,
                object_id=object_id,
                ra=ra,
                dec=dec,
                mjd=mjd,
                band=band,
                flux=flux,
                flux_err=flux_err,
                snr=snr,
                visit=int(visit_idx),
            )

            # diaObject grows with each detection
            first_mjd = detections[0]["midpointMjdTai"] if detections else mjd
            dia_obj = _dia_object(
                object_id=object_id,
                ra=ra,
                dec=dec,
                first_mjd=first_mjd,
                last_mjd=mjd,
                n_sources=detection_count,
            )

            alert = _alert(
                candid=candid,
                dia_source=dia_src,
                prv_dia_sources=list(detections),
                prv_dia_forced_sources=list(non_detections),
                dia_object=dia_obj,
            )
            alerts.append(alert)
            detections.append(dia_src)

        else:
            # Non-detection: only add if mag_ulim is available
            mag_ulim = row.get("mag_ulim")
            if pd.isna(mag_ulim):
                continue

            flux_err = mag_ulim_to_flux_err(float(mag_ulim))
            forced_src = _dia_forced_source(
                forced_id=object_id * 100_000 + int(visit_idx),
                object_id=object_id,
                ra=ra,
                dec=dec,
                mjd=mjd,
                band=band,
                flux_err=flux_err,
                visit=int(visit_idx),
            )
            non_detections.append(forced_src)

    return alerts


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------


def reset_topic(broker: str, topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": broker})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        fs = admin.delete_topics([topic], operation_timeout=30)
        for t, f in fs.items():
            try:
                f.result()
                print(f"Deleted topic '{t}'")
            except Exception as e:
                print(f"Could not delete '{t}': {e}")
        time.sleep(2)
    fs = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)],
        operation_timeout=30,
    )
    for t, f in fs.items():
        try:
            f.result()
            print(f"Created topic '{t}'")
        except Exception as e:
            print(f"Could not create '{t}': {e}")


# ---------------------------------------------------------------------------
# Parallel serialization helpers
# ---------------------------------------------------------------------------

_worker_schema = None
_worker_sr_header = None


def _worker_init(schema_version: str, schema_id: int) -> None:
    global _worker_schema, _worker_sr_header
    sr = packet.SchemaRegistry.from_filesystem()
    _worker_schema = sr.get_by_version(schema_version)
    _worker_sr_header = b"\x00" + struct.pack(">I", schema_id)


def _serialize_file(args: tuple) -> tuple[str, int | None, list[bytes]]:
    csv_path, lc_index, ra, dec = args
    alerts = alerts_from_csv(Path(csv_path), lc_index, ra, dec)
    if not alerts:
        return (csv_path, None, [])
    object_id = alerts[0]["diaSource"]["diaObjectId"]
    payloads = [_worker_sr_header + _worker_schema.serialize(a) for a in alerts]
    return (csv_path, object_id, payloads)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Produce simulated LSST alerts from CSV files to Kafka"
    )
    parser.add_argument(
        "--broker", default="localhost:9092", help="Kafka broker address"
    )
    parser.add_argument("--topic", default=KAFKA_TOPIC, help="Kafka topic name")
    parser.add_argument(
        "--data-dir", default=str(DATA_DIR), help="Directory containing CSV files"
    )
    parser.add_argument(
        "--limit", type=int, default=0, help="Max objects to produce (0 = no limit)"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete and recreate the Kafka topic before producing",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=mp.cpu_count(),
        help="Number of parallel serialization workers (default: CPU count)",
    )
    args = parser.parse_args()

    if args.reset:
        reset_topic(args.broker, args.topic)

    producer = Producer({"bootstrap.servers": args.broker, "linger.ms": 5})

    data_dir = Path(args.data_dir)
    csv_files = sorted(data_dir.glob("lightcurve_LSSTlike_*.csv"), key=lambda f: int(f.stem.split("_")[-1]))
    if not csv_files:
        print(f"No lightcurve CSV files found in {data_dir}")
        return

    if args.limit > 0:
        csv_files = csv_files[: args.limit]

    print("Loading field position lookup tables...")
    position_lookup = _load_position_lookup(data_dir)

    tasks = [
        (str(f), int(f.stem.split("_")[-1]), *position_lookup.get(int(f.stem.split("_")[-1]), (0.0, 0.0)))
        for f in csv_files
    ]

    print(f"Serializing {len(tasks)} objects with {args.workers} workers...")
    total = 0
    object_count = 0
    with mp.Pool(
        processes=args.workers,
        initializer=_worker_init,
        initargs=(SCHEMA_VERSION, SCHEMA_ID),
    ) as pool:
        for csv_path, object_id, payloads in pool.imap_unordered(_serialize_file, tasks):
            if not payloads:
                object_count += 1
                continue
            name = Path(csv_path).name
            print(f"[{name}] object_id={object_id} {len(payloads)} alerts", flush=True)
            for payload in payloads:
                producer.produce(args.topic, value=payload)
                total += 1
                if total % 100 == 0:
                    producer.poll(0)
            object_count += 1

    producer.flush()
    print(f"\nDone. {total} alerts produced to '{args.topic}'")


if __name__ == "__main__":
    main()
