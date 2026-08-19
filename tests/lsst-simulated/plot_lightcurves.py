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

Without --data-dir: plots passing objects from photometry.csv only (partial history).
With --data-dir: plots the complete raw lightcurve for every object in the simulated
range, with filter-passing detections highlighted. Rejected objects are saved as
{object_id}_rejected.png.

Usage:
    python plot_lightcurves.py --photometry photometry.csv --stats stats.csv --output-dir plots/
    uv run tests/lsst-simulated/plot_lightcurves.py --photometry photometry.csv --stats stats.csv --data-dir datav2 --limit 200 --output-dir plots/
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
        _fallback_seen[band] = FALLBACK_COLORS[
            len(_fallback_seen) % len(FALLBACK_COLORS)
        ]
    return _fallback_seen[band]


def plot_object(
    object_id: str, df: pd.DataFrame, output_path: Path, title_extra: str = ""
) -> None:
    all_bands = [b for b in BANDS if b in df["band"].values]
    extra_bands = sorted(b for b in df["band"].unique() if b not in BANDS)
    ordered_bands = all_bands + extra_bands

    n_bands = len(ordered_bands)
    if n_bands == 0:
        return

    n_rows = (n_bands + 1) // 2
    fig, axes = plt.subplots(n_rows, 2, figsize=(12, 3 * n_rows + 1), sharex=True)
    axes = axes.flatten() if n_rows > 1 else list(axes) + [plt.Axes(fig, [0, 0, 0, 0])]

    if n_bands % 2 == 1:
        axes[-1].set_visible(False)

    mjd_min = df["mjd"].min()
    mjd_max = df["mjd"].max()
    pad = (mjd_max - mjd_min) * 0.05 or 1.0

    n_passed = int(df["passed_filter"].sum())

    for band, ax in zip(ordered_bands, axes):
        color = get_color(band)
        ax.invert_yaxis()
        ax.set_ylabel(f"{band}\nmag")
        ax.set_xlabel("MJD")

        ax.set_xlim(mjd_min - pad, mjd_max + pad)
        ax.xaxis.get_major_formatter().set_useOffset(False)
        ax.tick_params(labelbottom=True)
        band_df = df[df["band"] == band]
        norm = band_df[~band_df["passed_filter"] & band_df["mag"].notna()]
        if not norm.empty:
            ax.errorbar(
                norm["mjd"],
                norm["mag"],
                yerr=norm["mag_err"].fillna(0),
                fmt="o",
                color=color,
                alpha=0.7,
                markersize=5,
                linewidth=1,
                capsize=2,
                zorder=3,
            )

        high = band_df[band_df["passed_filter"] & band_df["mag"].notna()]
        if not high.empty:
            ax.errorbar(
                high["mjd"],
                high["mag"],
                yerr=high["mag_err"].fillna(0),
                fmt="o",
                color="black",
                markeredgecolor="yellow",
                markeredgewidth=1.5,
                markersize=8,
                linewidth=1,
                capsize=2,
                zorder=4,
                label="passed filter",
            )
            ax.legend(loc="best", fontsize=7)

    fig.suptitle(
        f"Object {object_id}{title_extra}  –  {n_passed} detection(s) passed filter"
    )
    fig.tight_layout()
    fig.savefig(output_path, dpi=120)
    plt.close(fig)


def load_raw_lightcurve(filepath: Path) -> pd.DataFrame:
    """Load a raw lightcurve CSV into a standard format with a 'seq' column."""
    raw = pd.read_csv(filepath)

    if "time_mjd" in raw.columns:
        # v2: all rows are detections, band is raw (u/g/r/...), no mag_err
        det = raw.rename(columns={"time_mjd": "mjd"}).copy()
        det["band"] = "lsst" + det["band"].astype(str)
        if "mag_err" not in det.columns:
            det["mag_err"] = float("nan")
    elif "observationStartMJD" in raw.columns:
        # v1: filter to detected=True rows
        det = raw[raw["detected"].astype(str).str.lower() == "true"].copy()
        det = det.rename(columns={"observationStartMJD": "mjd", "mag_obs": "mag"})
        if "fiesta_filter" in det.columns:
            det["band"] = det["fiesta_filter"]
        else:
            det["band"] = "lsst" + det["band"].astype(str)
    else:
        return pd.DataFrame()

    det["seq"] = det["candidate_id"].apply(lambda x: int(str(x).split("_")[1]))
    det["passed_filter"] = False
    return det[["mjd", "band", "mag", "mag_err", "passed_filter", "seq"]]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--photometry", default="photometry.csv")
    parser.add_argument(
        "--stats", default=None, help="stats.csv from extract_output.py"
    )
    parser.add_argument("--output-dir", default="plots")
    parser.add_argument(
        "--data-dir",
        default=None,
        help=(
            "Raw lightcurve CSV directory. When given, all objects in the simulated "
            "range are plotted from their full raw lightcurve. Passing objects get "
            "filter-pass highlights; rejected objects are saved as {id}_rejected.png."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Match the --limit passed to the producer (0 = no limit).",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.photometry)
    df["passed_filter"] = df["passed_filter"].astype(str).str.lower() == "true"
    df["candid"] = pd.to_numeric(df["candid"], errors="coerce")

    # Build per-object metadata from stats.csv when available
    stats_by_id: dict[str, dict] = {}
    if args.stats:
        stats_df = pd.read_csv(args.stats)
        for _, row in stats_df.iterrows():
            stats_by_id[str(row["object_id"])] = row.to_dict()

    # Passing IDs: from stats.csv if available (authoritative), else from photometry
    if stats_by_id:
        passing_ids = set(stats_by_id.keys())
    else:
        passing_ids = {str(oid) for oid in df["object_id"].unique()}

    # Build passing seqs per object: seq = candid % 100_000
    passing_seqs_by_id: dict[str, set[int]] = {}
    for oid in passing_ids:
        sub = df[df["object_id"].astype(str) == oid]
        candids = sub.loc[sub["passed_filter"] & sub["candid"].notna(), "candid"]
        passing_seqs_by_id[oid] = {int(c) % 100_000 for c in candids}

    if args.data_dir:
        data_dir = Path(args.data_dir)
        csv_files = sorted(
            data_dir.glob("lightcurve_*.csv"),
            key=lambda f: int(f.stem.split("_")[-1]),
        )
        if args.limit > 0:
            csv_files = csv_files[: args.limit]

        def _oid(f: Path) -> str:
            s = int(f.stem.split("_")[-1])
            return str(s if s != 0 else 1_000_000)

        passing_files = [(f, True) for f in csv_files if _oid(f) in passing_ids]
        rejected_files = [(f, False) for f in csv_files if _oid(f) not in passing_ids]

        print(
            f"Plotting {len(passing_files)} passing + {len(rejected_files)} rejected objects into {output_dir}/"
        )

        for filepath, is_passing in passing_files + rejected_files:
            suffix = int(filepath.stem.split("_")[-1])
            object_id = str(suffix if suffix != 0 else 1_000_000)

            try:
                det = load_raw_lightcurve(filepath)
            except Exception as e:
                print(f"  Warning: could not read {filepath.name}: {e}")
                continue

            if det.empty:
                continue

            if is_passing:
                seqs = passing_seqs_by_id.get(object_id, set())
                det["passed_filter"] = det["seq"].isin(seqs)
                meta = stats_by_id.get(object_id, {})
                title_extra = ""
                target_name = meta.get("target_name")
                if target_name and str(target_name).lower() != "nan":
                    title_extra += f"  {target_name}"
                filter_names = meta.get("filter_names")
                if filter_names and str(filter_names).lower() != "nan":
                    title_extra += f"  [{filter_names}]"
                out = output_dir / f"{object_id}.png"
                plot_object(object_id, det, out, title_extra=title_extra)
                print(f"  {object_id}.png")
            else:
                out = output_dir / f"{object_id}_rejected.png"
                plot_object(object_id, det, out)
                print(f"  {object_id}_rejected.png")

    else:
        # Fallback: plot passing objects from photometry.csv only (partial history)
        object_ids = sorted(passing_ids, key=lambda x: int(x))
        print(f"Plotting {len(object_ids)} passing objects into {output_dir}/")
        for object_id in object_ids:
            sub = df[df["object_id"].astype(str) == object_id]
            if sub.empty:
                print(f"  {object_id} — no photometry data, skipping")
                continue
            meta = stats_by_id.get(object_id, {})
            title_extra = ""
            target_name = meta.get("target_name")
            if target_name and str(target_name).lower() != "nan":
                title_extra += f"  {target_name}"
            filter_names = meta.get("filter_names")
            if filter_names and str(filter_names).lower() != "nan":
                title_extra += f"  [{filter_names}]"
            plot_object(
                object_id, sub, output_dir / f"{object_id}.png", title_extra=title_extra
            )
            print(f"  {object_id}.png")

    print("Done.")


if __name__ == "__main__":
    main()
