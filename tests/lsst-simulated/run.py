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
parser.add_argument(
    "--config", default=None, help="Path to simulation config YAML file"
)
parser.add_argument("--keep-up", action="store_true", help="Keep sim services up after test")
# CLI overrides (all default to None so config values take precedence over hardcoded defaults)
parser.add_argument("--timeout", type=int, default=None, help="Timeout in seconds")
parser.add_argument("--data-dir", default=None, help="Path to lightcurve data directory")
parser.add_argument("--limit", type=int, default=None, help="Max objects to produce (0 = no limit)")
parser.add_argument("--filter-file", default=None, help="Path to filter JSON file")
parser.add_argument("--nickname", default=None, help="Short prefix for the simulation ID")
parser.add_argument("--workers", type=int, default=None, help="Producer parallelism (default: CPU count)")
parser.add_argument("--n-alert-workers", type=int, default=None, help="Alert worker count")
parser.add_argument("--n-enrichment-workers", type=int, default=None, help="Enrichment worker count")
parser.add_argument("--n-filter-workers", type=int, default=None, help="Filter worker count")
args = parser.parse_args()

BOOM_REPO_ROOT = os.path.abspath(args.boom_repo_dir)
TEST_DIR = os.path.join(BOOM_REPO_ROOT, "tests", "lsst-simulated")

# Load config file if provided
cfg: dict = {}
if args.config:
    with open(args.config) as f:
        cfg = yaml.safe_load(f) or {}


def resolve(cli_val, cfg_key, default):
    """CLI arg > config file > hardcoded default."""
    if cli_val is not None:
        return cli_val
    return cfg.get(cfg_key, default)


nickname = resolve(args.nickname, "nickname", None)
timeout = resolve(args.timeout, "timeout", 600)
limit = resolve(args.limit, "limit", 0)
ids = cfg.get("ids", None)
obj_range = cfg.get("range", None)
workers = resolve(args.workers, "workers", None)
n_alert_workers = resolve(args.n_alert_workers, "n_alert_workers", 4)
n_enrichment_workers = resolve(args.n_enrichment_workers, "n_enrichment_workers", 6)
n_filter_workers = resolve(args.n_filter_workers, "n_filter_workers", 2)

default_filter = os.path.join(TEST_DIR, "filter_KN_PRODUCTION.json")
filter_file = resolve(args.filter_file, "filter_file", default_filter)

default_data_dir = os.path.join(BOOM_REPO_ROOT, "data_alex")
DATA_DIR = os.path.abspath(resolve(args.data_dir, "data_dir", default_data_dir))

# Build simulation ID: {nickname}_{YYYYMMDD}_{HHMM} or {YYYYMMDD}_{HHMM}
now = datetime.now(timezone.utc)
date_str = now.strftime("%Y%m%d_%H%M%S")
SIM_ID = (f"{nickname}_{date_str}" if nickname else date_str).lower()
DB_NAME = f"boom-simu-{SIM_ID}"

print(f"Simulation ID : {SIM_ID}")
print(f"Database name : {DB_NAME}")

# Write per-sim config.yaml derived from the main config
SIMS_DIR = os.path.join(TEST_DIR, "sims")
os.makedirs(SIMS_DIR, exist_ok=True)
SIM_CONFIG_PATH = os.path.join(SIMS_DIR, f"config-{SIM_ID}.yaml")

with open(os.path.join(BOOM_REPO_ROOT, "config.yaml"), "r") as f:
    boom_config = yaml.safe_load(f)

boom_config["database"]["name"] = DB_NAME
boom_config["database"]["host"] = "mongo"
boom_config["database"]["password"] = "mongoadminsecret"
boom_config["kafka"]["consumer"]["lsst"]["server"] = "broker:29092"
boom_config["kafka"]["consumer"]["lsst"]["username"] = None
boom_config["kafka"]["consumer"]["lsst"]["password"] = None
boom_config["kafka"]["consumer"]["lsst"]["group_id"] = f"lsst-simulated-{SIM_ID}"
boom_config["kafka"]["consumer"]["lsst"]["schema_github_fallback_url"] = (
    "https://github.com/lsst/alert_packet/tree/w.2026.19/python/lsst/alert/packet/schema"
)
boom_config["kafka"]["consumer"]["lsst"].pop("schema_registry", None)
boom_config["kafka"]["producer"]["server"] = "broker:29092"
boom_config["redis"]["host"] = "valkey"
boom_config["cutouts_storage"]["host"] = "mongo"
boom_config["cutouts_storage"]["password"] = "mongoadminsecret"
boom_config["cutouts_storage"]["name"] = DB_NAME
boom_config["api"]["auth"]["secret_key"] = "lsst-simulated-test-secret-key-32chars"
boom_config["api"]["auth"]["admin_password"] = "adminsecret"
boom_config["workers"]["lsst"]["alert"]["n_workers"] = n_alert_workers
boom_config["workers"]["lsst"]["enrichment"]["n_workers"] = n_enrichment_workers
boom_config["workers"]["lsst"]["filter"]["n_workers"] = n_filter_workers
# Simulated data doesn't need external catalog lookups — clear crossmatches
# so enrichment never tries to reach NED/LSPSC over the network.
boom_config["crossmatch"]["lsst"] = []

with open(SIM_CONFIG_PATH, "w") as f:
    yaml.safe_dump(boom_config, f, default_flow_style=False, sort_keys=False)

print(f"Generated config: {SIM_CONFIG_PATH}")

# Resolve which CSV files to produce
all_csv_files = sorted(
    Path(DATA_DIR).glob("lightcurve_*.csv"), key=lambda f: int(f.stem.split("_")[-1])
)

if ids is not None:
    id_set = set(ids)
    csv_files = [f for f in all_csv_files if int(f.stem.split("_")[-1]) in id_set]
elif obj_range is not None:
    start, end = obj_range[0], obj_range[1]
    csv_files = [f for f in all_csv_files if start <= int(f.stem.split("_")[-1]) <= end]
else:
    csv_files = all_csv_files[:limit] if limit > 0 else all_csv_files

if not csv_files:
    raise SystemExit(f"No lightcurve files found in {DATA_DIR} matching the requested selection.")

n_objects = len(csv_files)
lc_indices = {int(f.stem.split("_")[-1]) for f in csv_files}
producer_ids = ",".join(str(i) for i in sorted(lc_indices))

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
    ["docker", "compose", "--progress", "quiet", *INFRA_COMPOSE, "up", "-d", "--wait"],
    check=True,
)

# --- Run the simulation ---
SIM_PROJECT = f"boom-sim-{SIM_ID}"
os.environ["BOOM_REPO_ROOT"] = BOOM_REPO_ROOT
os.environ["DATA_DIR_HOST"] = DATA_DIR
os.environ["PRODUCER_IDS"] = producer_ids
os.environ["TIMEOUT_SECS"] = str(timeout)
os.environ["EXPECTED_ALERTS"] = str(expected_alerts)
os.environ["SIM_ID"] = SIM_ID
os.environ["SIM_DB_NAME"] = DB_NAME
os.environ["SIM_CONFIG_PATH"] = SIM_CONFIG_PATH
os.environ["SIM_PROJECT"] = SIM_PROJECT
if workers is not None:
    os.environ["PRODUCER_WORKERS"] = str(workers)
if filter_file:
    filter_path = os.path.abspath(filter_file)
    if not os.path.isfile(filter_path):
        raise FileNotFoundError(f"Filter file not found: {filter_path}")
    os.environ["FILTER_FILE"] = filter_path

cmd = ["bash", os.path.join(TEST_DIR, "_run.sh")]
if args.keep_up:
    cmd.append("--keep-up")

subprocess.run(cmd, check=True)
