#!/usr/bin/env python3
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "matplotlib",
#     "pandas",
# ]
# ///
"""
Plot detection light curves from extract_output.py CSV output.

One PNG per object: one color per band, error bars on every detection,
and a black open circle around detections that triggered the filter pass.

Usage:
    python plot_lightcurves.py [--photometry photometry.csv] [--output-dir plots/]
    uv run tests/lsst-simulated/plot_lightcurves.py --photometry photometry.csv --output-dir plots/
"""

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

BANDS = ["lsstu", "lsstg", "lsstr", "lssti", "lsstz", "lssty"]

BAND_COLORS = {
    "lsstu": "purple",
    "lsstg": "green",
    "lsstr": "red",
    "lssti": "orange",
    "lsstz": "brown",
    "lssty": "gray",
}
FALLBACK_COLORS = ["#3498DB", "#E91E63", "#FF5722", "#607D8B", "#795548"]

_fallback_seen: dict[str, str] = {}


def get_color(band: str) -> str:
    if band in BAND_COLORS:
        return BAND_COLORS[band]
    if band not in _fallback_seen:
        _fallback_seen[band] = FALLBACK_COLORS[len(_fallback_seen) % len(FALLBACK_COLORS)]
    return _fallback_seen[band]


def plot_object(object_id: str, df: pd.DataFrame, output_dir: Path) -> None:
    all_bands = [b for b in BANDS if b in df["band"].values]
    extra_bands = sorted(b for b in df["band"].unique() if b not in BANDS)
    ordered_bands = all_bands + extra_bands

    n_bands = len(ordered_bands)
    n_rows = (n_bands + 1) // 2
    fig, axes = plt.subplots(n_rows, 2, figsize=(12, 3 * n_rows + 1))
    axes = axes.flatten() if n_rows > 1 else list(axes) + [plt.Axes(fig, [0, 0, 0, 0])]

    # Hide unused subplot if odd number of bands
    if n_bands % 2 == 1:
        axes[-1].set_visible(False)

    n_passed = int(df["passed_filter"].sum())

    for i, (band, ax) in enumerate(zip(ordered_bands, axes)):
        color = get_color(band)
        ax.invert_yaxis()
        ax.set_ylabel(f"{band}\nmag")
        ax.set_xlabel("MJD")

        band_df = df[df["band"] == band]
        norm = band_df[~band_df["passed_filter"] & band_df["mag"].notna()]
        if not norm.empty:
            ax.errorbar(
                norm["mjd"], norm["mag"],
                yerr=norm["mag_err"].fillna(0),
                fmt="o", color=color, alpha=0.7,
                markersize=5, linewidth=1, capsize=2, zorder=3,
            )

        high = band_df[band_df["passed_filter"] & band_df["mag"].notna()]
        if not high.empty:
            ax.errorbar(
                high["mjd"], high["mag"],
                yerr=high["mag_err"].fillna(0),
                fmt="o", color="black",
                markeredgecolor="yellow", markeredgewidth=1.5,
                markersize=8, linewidth=1, capsize=2, zorder=4,
                label="passed filter",
            )
            ax.legend(loc="best", fontsize=7)

    fig.suptitle(f"Object {object_id}  –  {n_passed} detection(s) passed filter")
    fig.tight_layout()
    fig.savefig(output_dir / f"{object_id}.png", dpi=120)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--photometry", default="photometry.csv")
    parser.add_argument("--output-dir", default="plots")
    args = parser.parse_args()

    df = pd.read_csv(args.photometry)
    df["passed_filter"] = df["passed_filter"].astype(str).str.lower() == "true"

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    object_ids = df["object_id"].unique()
    print(f"Plotting {len(object_ids)} objects into {output_dir}/")

    for i, object_id in enumerate(sorted(object_ids), 1):
        sub = df[df["object_id"] == object_id]
        plot_object(str(object_id), sub, output_dir)
        print(f"  [{i}/{len(object_ids)}] {object_id}.png")

    print("Done.")


if __name__ == "__main__":
    main()
