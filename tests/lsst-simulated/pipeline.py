#!/usr/bin/env python3
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pyyaml",
#     "pandas>2",
# ]
# ///
"""
Full simulation pipeline: run → extract → plot → teardown.

Runs the simulation with --keep-up, captures the sim_id from output,
then runs extract_output.py and plot_lightcurves.py automatically.

Usage:
    uv run tests/lsst-simulated/pipeline.py --config tests/lsst-simulated/example-config.yaml
    uv run tests/lsst-simulated/pipeline.py --nickname cdo --data-dir /path/to/dataset --limit 200
    uv run tests/lsst-simulated/pipeline.py --config tests/lsst-simulated/example-config.yaml --keep-up
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
REPO_ROOT = HERE.parent.parent


def run_streaming(cmd: list[str], capture_prefix: str) -> str | None:
    """Run a command, stream its output to stdout, and return the value after capture_prefix."""
    captured = None
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    assert process.stdout is not None
    for line in process.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        if line.startswith(capture_prefix):
            captured = line[len(capture_prefix):].strip()
    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, cmd)
    return captured


def main() -> None:
    parser = argparse.ArgumentParser(description="Full simulation pipeline: run → extract → plot")
    parser.add_argument("--boom-repo-dir", default=".", help="Path to the boom repo directory")
    parser.add_argument("--config", default=None, help="Path to simulation config YAML file")
    parser.add_argument("--nickname", default=None, help="Short prefix for the simulation ID")
    parser.add_argument("--data-dir", default=None, help="Path to lightcurve data directory")
    parser.add_argument("--limit", type=int, default=None, help="Max objects to produce (0 = no limit)")
    parser.add_argument("--filter-file", default=None, help="Path to filter JSON file")
    parser.add_argument("--timeout", type=int, default=None, help="Simulation timeout in seconds")
    parser.add_argument("--workers", type=int, default=None, help="Producer parallelism")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka broker for extraction")
    parser.add_argument("--no-plot", action="store_true", help="Skip plotting step")
    parser.add_argument("--keep-up", action="store_true", help="Keep sim services up after pipeline")
    args = parser.parse_args()

    boom_repo = os.path.abspath(args.boom_repo_dir)

    # --- Step 1: run simulation (always --keep-up so Kafka is available for extraction) ---
    print("\n" + "=" * 60)
    print("STEP 1 / 3  —  Running simulation")
    print("=" * 60 + "\n")

    run_cmd = [
        "uv", "run", str(HERE / "run.py"),
        "--boom-repo-dir", boom_repo,
        "--keep-up",
    ]
    if args.config:
        run_cmd += ["--config", args.config]
    if args.nickname:
        run_cmd += ["--nickname", args.nickname]
    if args.data_dir:
        run_cmd += ["--data-dir", args.data_dir]
    if args.limit is not None:
        run_cmd += ["--limit", str(args.limit)]
    if args.filter_file:
        run_cmd += ["--filter-file", args.filter_file]
    if args.timeout:
        run_cmd += ["--timeout", str(args.timeout)]
    if args.workers:
        run_cmd += ["--workers", str(args.workers)]

    sim_id = run_streaming(run_cmd, "Simulation ID : ")
    if not sim_id:
        raise SystemExit("Could not capture simulation ID from run.py output")

    print(f"\nCaptured sim_id: {sim_id}")

    # --- Step 2: extract results ---
    print("\n" + "=" * 60)
    print("STEP 2 / 3  —  Extracting results")
    print("=" * 60 + "\n")

    extract_cmd = [
        "uv", "run", str(HERE / "extract_output.py"),
        "--sim-id", sim_id,
        "--broker", args.broker,
    ]
    if args.config:
        extract_cmd += ["--config", args.config]
    if args.data_dir:
        extract_cmd += ["--data-dir", args.data_dir]
    if args.limit is not None:
        extract_cmd += ["--limit", str(args.limit)]

    subprocess.run(extract_cmd, check=True)

    # --- Step 3: plot ---
    if not args.no_plot:
        print("\n" + "=" * 60)
        print("STEP 3 / 3  —  Plotting lightcurves")
        print("=" * 60 + "\n")

        plot_cmd = [
            "uv", "run", str(HERE / "plot_lightcurves.py"),
            "--sim-id", sim_id,
        ]
        if args.config:
            plot_cmd += ["--config", args.config]
        if args.data_dir:
            plot_cmd += ["--data-dir", args.data_dir]
        if args.limit is not None:
            plot_cmd += ["--limit", str(args.limit)]

        subprocess.run(plot_cmd, check=True)

    # --- Teardown ---
    if not args.keep_up:
        print(f"\nTearing down sim stack for {sim_id}...")
        subprocess.run(
            [
                "docker", "compose", "--progress", "quiet",
                "-p", f"boom-sim-{sim_id}",
                "-f", str(HERE / "sim" / "compose.yaml"),
                "down", "--volumes",
            ],
            check=False,
        )

    print(f"\nDone. Results in results/{sim_id}/  —  Plots in plots/{sim_id}/")


if __name__ == "__main__":
    main()
