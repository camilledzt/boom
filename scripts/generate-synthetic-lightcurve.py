#!/usr/bin/env python3
"""
Generate a synthetic KN-like lightcurve CSV compatible with produce-lsst-simulated.py.

The lightcurve rises over ~2 days then fades over ~5 days, with detections in
multiple bands spread over multiple nights — enough for the KN filter's
rising/fading rate computation (nb_data >= 2 per branch).

Usage:
    python scripts/generate-synthetic-lightcurve.py \
        --output data_alex/lightcurve_LSSTlike_0100.csv \
        --index 100 \
        --t0 63000.0
"""

import argparse
import math
import numpy as np
import pandas as pd

BANDS = ["g", "r", "i", "z"]

# KN-like magnitude evolution: rise then fade per band
# (peak_offset_days, rise_rate_mag/day, fade_rate_mag/day, peak_mag)
BAND_PARAMS = {
    "g": dict(peak_offset=1.0, rise_rate=0.8, fade_rate=0.5, peak_mag=21.5),
    "r": dict(peak_offset=1.5, rise_rate=0.6, fade_rate=0.4, peak_mag=21.2),
    "i": dict(peak_offset=2.0, rise_rate=0.5, fade_rate=0.3, peak_mag=21.0),
    "z": dict(peak_offset=2.5, rise_rate=0.4, fade_rate=0.25, peak_mag=21.1),
}

# Observation schedule: (day_offset, band)
SCHEDULE = [
    (0.0, "g"), (0.0, "r"), (0.0, "i"), (0.0, "z"),
    (0.5, "g"), (0.5, "r"),
    (1.0, "g"), (1.0, "r"), (1.0, "i"), (1.0, "z"),
    (1.5, "r"), (1.5, "i"),
    (2.0, "g"), (2.0, "r"), (2.0, "i"), (2.0, "z"),
    (3.0, "r"), (3.0, "i"), (3.0, "z"),
    (4.0, "r"), (4.0, "i"), (4.0, "z"),
    (5.0, "g"), (5.0, "r"), (5.0, "i"),
    (7.0, "r"), (7.0, "i"), (7.0, "z"),
    (10.0, "r"), (10.0, "i"),
]

LIMITING_MAG = 24.5
SNR_THRESHOLD = 5.0


def mag_at(band: str, t_days: float) -> float:
    p = BAND_PARAMS[band]
    dt = t_days - p["peak_offset"]
    if dt < 0:
        return p["peak_mag"] + p["rise_rate"] * abs(dt)
    else:
        return p["peak_mag"] + p["fade_rate"] * dt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--index", type=int, default=100)
    parser.add_argument("--t0", type=float, default=63000.0, help="MJD of t=0")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)

    rows = []
    for t_offset, band in SCHEDULE:
        mjd = args.t0 + t_offset + rng.uniform(0, 0.01)  # small intra-night jitter
        mag_true = mag_at(band, t_offset)
        m5 = LIMITING_MAG + rng.uniform(-0.2, 0.2)

        snr_exp = 10 ** (0.4 * (m5 - mag_true)) / 5.0 * SNR_THRESHOLD
        mag_err = 2.5 / (math.log(10) * max(snr_exp, 0.1))
        mag_obs = mag_true + rng.normal(0, mag_err)
        snr_obs = snr_exp * rng.uniform(0.8, 1.2)

        detected = snr_obs >= SNR_THRESHOLD and mag_true <= LIMITING_MAG

        rows.append({
            "observationStartMJD": mjd,
            "band": band,
            "m5": m5,
            "t_days": t_offset,
            "fiesta_filter": f"lsst{band}",
            "mag_true": mag_true,
            "snr_exp": snr_exp,
            "mag_err": mag_err,
            "snr_obs": snr_obs,
            "detected": detected,
            "mag_obs": mag_obs if detected else float("nan"),
            "mag_ulim": m5 if not detected else float("nan"),
        })

    df = pd.DataFrame(rows)
    df.to_csv(args.output, index=False)
    n_det = df["detected"].sum()
    print(f"Written {args.output}: {len(df)} observations, {n_det} detections")
    for band in BANDS:
        b = df[(df["band"] == band) & df["detected"]]
        print(f"  {band}: {len(b)} detections, MJD range {b['observationStartMJD'].min():.2f}–{b['observationStartMJD'].max():.2f}" if len(b) else f"  {band}: 0 detections")


if __name__ == "__main__":
    main()
