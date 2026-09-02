"""Run the LSST simulated alert pipeline test."""
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pyyaml",
#     "pandas>2",
# ]
# ///

import argparse
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

parser = argparse.ArgumentParser(description="Run LSST simulated alert pipeline test")
parser.add_argument(
    "--boom-repo-dir", default=".", help="Path to the boom repo directory"
)
parser.add_argument("--timeout", type=int, default=600, help="Timeout in seconds")
parser.add_argument(
    "--keep-up", action="store_true", help="Keep sim services up after test"
)
parser.add_argument(
    "--data-dir",
    default=None,
    help="Path to lightcurve data directory (default: <boom-repo-dir>/data_alex)",
)
parser.add_argument(
    "--limit", type=int, default=0, help="Max objects to produce (0 = no limit)"
)
parser.add_argument(
    "--filter-file",
    default=os.path.join(os.path.dirname(__file__), "filter_KN_PRODUCTION.json"),
    help="Path to filter JSON file to load into the test database",
)
parser.add_argument(
    "--nickname",
    default=None,
    help="Short prefix for the simulation ID (e.g. 'cdo' → 'cdo_20260819_1430')",
)
args = parser.parse_args()

BOOM_REPO_ROOT = os.path.abspath(args.boom_repo_dir)
TEST_DIR = os.path.join(BOOM_REPO_ROOT, "tests", "lsst-simulated")
DATA_DIR = (
    os.path.abspath(args.data_dir)
    if args.data_dir
    else os.path.join(BOOM_REPO_ROOT, "data_alex")
)

# Build simulation ID: {nickname}_{YYYYMMDD}_{HHMM} or {YYYYMMDD}_{HHMM}
now = datetime.now(timezone.utc)
date_str = now.strftime("%Y%m%d_%H%M%S")
SIM_ID = f"{args.nickname}_{date_str}" if args.nickname else date_str
# MongoDB names cannot contain slashes or dots; underscores are safe.
DB_NAME = f"boom-simu-{SIM_ID}"

print(f"Simulation ID : {SIM_ID}")
print(f"Database name : {DB_NAME}")

# Write per-sim config.yaml derived from the main config
SIMS_DIR = os.path.join(TEST_DIR, "sims")
os.makedirs(SIMS_DIR, exist_ok=True)
SIM_CONFIG_PATH = os.path.join(SIMS_DIR, f"config-{SIM_ID}.yaml")

with open(os.path.join(BOOM_REPO_ROOT, "config.yaml"), "r") as f:
    config = yaml.safe_load(f)

config["database"]["name"] = DB_NAME
config["database"]["host"] = "mongo"
config["database"]["password"] = "mongoadminsecret"
config["kafka"]["consumer"]["lsst"]["server"] = "broker:29092"
config["kafka"]["consumer"]["lsst"]["username"] = None
config["kafka"]["consumer"]["lsst"]["password"] = None
config["kafka"]["consumer"]["lsst"]["group_id"] = f"lsst-simulated-{SIM_ID}"
config["kafka"]["consumer"]["lsst"]["schema_github_fallback_url"] = (
    "https://github.com/lsst/alert_packet/tree/w.2026.19/python/lsst/alert/packet/schema"
)
config["kafka"]["consumer"]["lsst"].pop("schema_registry", None)
config["kafka"]["producer"]["server"] = "broker:29092"
config["redis"]["host"] = "valkey"
config["cutouts_storage"]["host"] = "mongo"
config["cutouts_storage"]["password"] = "mongoadminsecret"
config["cutouts_storage"]["name"] = DB_NAME
config["api"]["auth"]["secret_key"] = "lsst-simulated-test-secret-key-32chars"
config["api"]["auth"]["admin_password"] = "adminsecret"
config["workers"]["lsst"]["alert"]["n_workers"] = 4
config["workers"]["lsst"]["enrichment"]["n_workers"] = 6
config["workers"]["lsst"]["filter"]["n_workers"] = 2
# Simulated data doesn't need external catalog lookups — clear crossmatches
# so enrichment never tries to reach NED/LSPSC over the network.
config["crossmatch"]["lsst"] = []

with open(SIM_CONFIG_PATH, "w") as f:
    yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

print(f"Generated config: {SIM_CONFIG_PATH}")

# Count expected alerts
csv_files = sorted(
    Path(DATA_DIR).glob("lightcurve_*.csv"), key=lambda f: int(f.stem.split("_")[-1])
)
if args.limit > 0:
    csv_files = csv_files[: args.limit]
n_objects = len(csv_files)
lc_indices = {int(f.stem.split("_")[-1]) for f in csv_files}

summary_path = os.path.join(DATA_DIR, "summary.csv")
summary = pd.read_csv(summary_path)
matched = summary[summary["i"].isin(lc_indices)]
n_detected_col = (
    matched["n_detected"] if "n_detected" in matched.columns else pd.Series(dtype=float)
)
expected_alerts = int(n_detected_col.sum()) if n_detected_col.notna().any() else 0

if expected_alerts == 0:
    mag_col = "mag" if "mag" in pd.read_csv(csv_files[0]).columns else "mag_obs"
    expected_alerts = sum(
        len(pd.read_csv(f).dropna(subset=[mag_col])) for f in csv_files
    )

print(f"Expected alerts: {expected_alerts} (from {n_objects} lightcurve files)")

# --- Start shared infrastructure (idempotent) ---
INFRA_COMPOSE = ["-f", os.path.join(TEST_DIR, "infra", "compose.yaml")]
print("Starting shared infrastructure (broker, mongo, valkey)...")
subprocess.run(
    ["docker", "compose", *INFRA_COMPOSE, "up", "-d", "--wait"],
    check=True,
)

# --- Run the simulation ---
SIM_PROJECT = f"boom-sim-{SIM_ID}"
os.environ["BOOM_REPO_ROOT"] = BOOM_REPO_ROOT
os.environ["DATA_DIR_HOST"] = DATA_DIR
os.environ["PRODUCER_LIMIT"] = str(args.limit)
os.environ["TIMEOUT_SECS"] = str(args.timeout)
os.environ["EXPECTED_ALERTS"] = str(expected_alerts)
os.environ["SIM_ID"] = SIM_ID
os.environ["SIM_DB_NAME"] = DB_NAME
os.environ["SIM_CONFIG_PATH"] = SIM_CONFIG_PATH
os.environ["SIM_PROJECT"] = SIM_PROJECT
if args.filter_file:
    filter_path = os.path.abspath(args.filter_file)
    if not os.path.isfile(filter_path):
        raise FileNotFoundError(f"Filter file not found: {filter_path}")
    os.environ["FILTER_FILE"] = filter_path

cmd = ["bash", os.path.join(TEST_DIR, "_run.sh")]
if args.keep_up:
    cmd.append("--keep-up")

subprocess.run(cmd, check=True)
