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

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd

BAND_COLORS = {
    "lsstu": "purple",
    "lsstg": "green",
    "lsstr": "red",
    "lssti": "orange",
    "lsstz": "brown",
    "lssty": "gray",
}


def get_color(band: str, seen: dict) -> str:
    if band in BAND_COLORS:
        return BAND_COLORS[band]
    return seen[band]


def plot_object(object_id: str, df: pd.DataFrame, output_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.invert_yaxis()

    seen_colors: dict[str, str] = {}

    passed_mjds = df[df["passed_filter"] & df["mag"].notna()]["mjd"].tolist()
    if len(passed_mjds) >= 2:
        ax.axvspan(
            min(passed_mjds),
            max(passed_mjds),
            color="gray",
            alpha=0.2,
            zorder=1,
            label="Time range of interest",
        )

    for _, row in df.iterrows():
        if pd.isna(row["mag"]):
            continue
        color = get_color(row["band"], seen_colors)
        yerr = row["mag_err"] if pd.notna(row["mag_err"]) else 0
        if row["passed_filter"]:
            ax.errorbar(
                row["mjd"],
                row["mag"],
                yerr=yerr,
                fmt="o",
                color="black",
                markeredgecolor="yellow",
                markeredgewidth=1.5,
                markersize=8,
                linewidth=1,
                capsize=2,
                zorder=4,
            )
        else:
            ax.errorbar(
                row["mjd"],
                row["mag"],
                yerr=yerr,
                fmt="o",
                color=color,
                alpha=0.7,
                markersize=5,
                linewidth=1,
                capsize=2,
                zorder=3,
            )

    n_passed = int(df["passed_filter"].sum())
    ax.set_xlabel("MJD")
    ax.set_ylabel("Magnitude")
    ax.set_title(f"Object {object_id}  –  {n_passed} detection(s) passed filter")

    seen_bands = df["band"].unique()
    band_patches = [
        mpatches.Patch(color=get_color(b, seen_colors), label=b)
        for b in sorted(seen_bands)
    ]
    passed_marker = plt.Line2D(
        [],
        [],
        linestyle="none",
        marker="o",
        markerfacecolor="black",
        markeredgecolor="yellow",
        markeredgewidth=1.5,
        markersize=8,
        label="passed filter",
    )
    ax.legend(handles=band_patches + [passed_marker], loc="best", fontsize=8)

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
