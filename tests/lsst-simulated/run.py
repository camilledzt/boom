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
from pathlib import Path

import pandas as pd
import yaml

parser = argparse.ArgumentParser(description="Run LSST simulated alert pipeline test")
parser.add_argument("--boom-repo-dir", default=".", help="Path to the boom repo directory")
parser.add_argument("--timeout", type=int, default=300, help="Timeout in seconds")
parser.add_argument("--keep-up", action="store_true", help="Keep services up after test")
parser.add_argument("--data-dir", default=None, help="Path to lightcurve data directory (default: <boom-repo-dir>/data_alex)")
parser.add_argument("--limit", type=int, default=0, help="Max alerts to produce (0 = no limit)")
parser.add_argument(
    "--filter-file",
    default=os.path.join(os.path.dirname(__file__), "filter_KN_PRODUCTION.json"),
    help="Path to filter JSON file to load into the test database",
)
args = parser.parse_args()

BOOM_REPO_ROOT = os.path.abspath(args.boom_repo_dir)
TEST_DIR = os.path.join(BOOM_REPO_ROOT, "tests", "lsst-simulated")
DB_NAME = "boom-lsst-simulated"
DATA_DIR = os.path.abspath(args.data_dir) if args.data_dir else os.path.join(BOOM_REPO_ROOT, "data_alex")

# Generate config.yaml for the test (adapted from main config.yaml)
with open(os.path.join(BOOM_REPO_ROOT, "config.yaml"), "r") as f:
    config = yaml.safe_load(f)

config["database"]["name"] = DB_NAME
config["database"]["host"] = "mongo"
config["database"]["password"] = "mongoadminsecret"
config["kafka"]["consumer"]["lsst"]["server"] = "broker:29092"
config["kafka"]["consumer"]["lsst"]["username"] = ""
config["kafka"]["consumer"]["lsst"]["password"] = ""
config["kafka"]["consumer"]["lsst"]["group_id"] = "lsst-simulated-test"
config["kafka"]["consumer"]["lsst"]["schema_github_fallback_url"] = (
    "https://github.com/lsst/alert_packet/tree/w.2026.19/python/lsst/alert/packet/schema"
)
config["kafka"]["consumer"]["lsst"].pop("schema_registry", None)
config["kafka"]["producer"]["server"] = "broker:29092"
config["redis"]["host"] = "valkey"
config["cutouts_storage"]["host"] = "mongo"
config["cutouts_storage"]["password"] = "mongoadminsecret"
config["api"]["auth"]["secret_key"] = "lsst-simulated-test-secret-key-32chars"
config["api"]["auth"]["admin_password"] = "adminsecret"

with open(os.path.join(TEST_DIR, "config.yaml"), "w") as f:
    yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

print("Generated config.yaml")

# Count expected alerts: only consider lightcurve files that actually exist
data_dir = DATA_DIR
csv_files = sorted(Path(data_dir).glob("lightcurve_LSSTlike_*.csv"))
lc_indices = {int(f.stem.split("_")[-1]) for f in csv_files}
summary_path = os.path.join(data_dir, "summary.csv")
summary = pd.read_csv(summary_path)
matched = summary[summary["i"].isin(lc_indices) & (summary["n_detected"] > 0)]
expected_alerts = int(matched["n_detected"].sum())
if args.limit > 0:
    expected_alerts = min(expected_alerts, args.limit)
n_active = len(matched)
print(f"Expected alerts: {expected_alerts} (from {n_active}/{len(lc_indices)} lightcurve files with detections)")

# Run the test
os.environ["BOOM_REPO_ROOT"] = BOOM_REPO_ROOT
os.environ["DATA_DIR_HOST"] = DATA_DIR
os.environ["PRODUCER_LIMIT"] = str(args.limit)
os.environ["TIMEOUT_SECS"] = str(args.timeout)
os.environ["EXPECTED_ALERTS"] = str(expected_alerts)
os.environ["DB_NAME"] = DB_NAME
if args.filter_file:
    os.environ["FILTER_FILE"] = os.path.abspath(args.filter_file)

cmd = ["bash", os.path.join(TEST_DIR, "_run.sh")]
if args.keep_up:
    cmd.append("--keep-up")

subprocess.run(cmd, check=True)
