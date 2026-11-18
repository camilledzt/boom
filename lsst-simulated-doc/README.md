# LSST Simulated Alert Pipeline

---

## Prerequisites

- `data_alex/` directory at the repo root with:
  - `lightcurve_LSSTlike_*.csv` — per-object photometric time series
  - `summary.csv` — maps file index → `field_idx`
  - `opsim_fields_index.csv` — maps `field_index` → `ra0_deg`, `dec0_deg`
- `data/models/` and `data/ls_footprint_moc.fits` for the scheduler
- `.env` at the repo root (copy from `.env.example` and fill in secrets)
- Python virtual environment: `uv venv && uv pip install lsst-alert-packet confluent-kafka numpy pandas`

---

## Running the multi-simulation isolated stack (from scratch, Ubuntu/Debian)

The `multi_simulation` branch splits the test stack into two compose files:

- `tests/lsst-simulated/infra/compose.yaml` — shared infra started once:
  `mongo`, `valkey`, `broker`. Reused by every simulation.
- `tests/lsst-simulated/sim/compose.yaml` — per-simulation services
  (`producer`, `consumer`, `scheduler`, `add-filter`) launched under a unique
  Docker Compose project name `boom-sim-<SIM_ID>`.

Each run gets a `SIM_ID` of the form `<nickname>_YYYYMMDD_HHMMSS`, its own
MongoDB database `boom-simu-<SIM_ID>`, its own Kafka consumer group, and its
own generated boom config at `tests/lsst-simulated/sims/config-<SIM_ID>.yaml`.
You can launch several simulations in parallel against the same shared infra.

### 1. Install prerequisites (Ubuntu/Debian)

```bash
# Docker Engine + Compose v2 plugin
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg git
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker "$USER"   # re-login after this

# uv (Python runner used by run.py)
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Verify: `docker compose version` and `uv --version`.

### 2. Clone and prepare the repo

```bash
git clone https://github.com/<your-fork>/boom.git
cd boom
git checkout multi_simulation
cp .env.example .env   # fill in any required secrets
```

Required inputs at the repo root:

- a lightcurve dataset directory (e.g. `data_02092026_1/`) containing
  `lightcurve_LSSTlike_*.csv`, `summary.csv`, `opsim_fields_index.csv`
- `data/models/` and `data/ls_footprint_moc.fits` (scheduler assets)
- `config.yaml` (the base boom config — `run.py` patches a per-sim copy)

### 3. Build the boom image

The per-sim compose builds `boom-astro/boom:latest` itself. Do this once (and
again whenever Rust code changes):

```bash
BOOM_REPO_ROOT=$(pwd) docker compose \
  -f tests/lsst-simulated/sim/compose.yaml build --no-cache
```

### 4. Start the shared infrastructure

`run.py` starts it automatically on the first run, but you can also bring it
up explicitly and leave it running between simulations:

```bash
BOOM_REPO_ROOT=$(pwd) docker compose \
  -f tests/lsst-simulated/infra/compose.yaml up -d --wait
```

### 5. Launch a simulation

Write a per-sim YAML (see `tests/lsst-simulated/example-config.yaml`), then:

```bash
BOOM_REPO_ROOT=$(pwd) uv run tests/lsst-simulated/run.py \
    --boom-repo-dir . \
    --config tests/lsst-simulated/example-config.yaml \
    --keep-up
```

`run.py` will:
1. Compute `SIM_ID` from `nickname` + UTC timestamp and print it
2. Generate `tests/lsst-simulated/sims/config-<SIM_ID>.yaml` (patches DB name,
   broker address, Kafka group id, credentials, worker counts, crossmatches)
3. Resolve which lightcurve CSVs to produce (`ids`, `range`, or `limit`) and
   count expected alerts from `summary.csv`
4. Bring up the shared infra compose (idempotent)
5. Bring up the per-sim compose under project `boom-sim-<SIM_ID>`
   (producer → consumer → scheduler, plus a one-shot `add-filter`)
6. Poll MongoDB until all expected alerts are ingested and enriched
7. Tear the per-sim project down (unless `--keep-up`)

### 6. Launch more simulations in parallel

Reuse the shared infra and just invoke `run.py` again with a different
`nickname` (and optionally different dataset/filter). Each invocation gets
its own `SIM_ID`, DB, Kafka group, and compose project — they do not collide.

```bash
BOOM_REPO_ROOT=$(pwd) uv run tests/lsst-simulated/run.py \
    --boom-repo-dir . --config tests/lsst-simulated/example-config.yaml \
    --nickname run_a --keep-up &

BOOM_REPO_ROOT=$(pwd) uv run tests/lsst-simulated/run.py \
    --boom-repo-dir . --config tests/lsst-simulated/example-config.yaml \
    --nickname run_b --keep-up &
```

### 7. CLI flags

CLI flags always override config values. `--keep-up` and `--boom-repo-dir`
are runtime/infra flags and stay on the CLI only.

| Flag | Config key | Default | Description |
|------|------------|---------|-------------|
| `--boom-repo-dir` | — | `.` | Path to the repo root |
| `--config` | — | none | Per-sim YAML (see `example-config.yaml`) |
| `--nickname` | `nickname` | none | Short prefix used in `SIM_ID` / DB name |
| `--data-dir` | `data_dir` | `data_alex` | Lightcurve CSV directory |
| `--limit` | `limit` | `0` (all) | Max objects to produce |
| `--filter-file` | `filter_file` | `tests/lsst-simulated/filter_KN_PRODUCTION.json` | Filter JSON loaded by `add-filter` |
| `--workers` | `workers` | CPU count | Producer parallelism |
| `--n-alert-workers` | `n_alert_workers` | `4` | Alert workers inside boom |
| `--n-enrichment-workers` | `n_enrichment_workers` | `6` | Enrichment workers |
| `--n-filter-workers` | `n_filter_workers` | `2` | Filter workers |
| `--timeout` | `timeout` | `600` | Seconds before the test fails |
| `--keep-up` | — | off | Keep per-sim services running after the test |

Config-only keys: `ids: [i, j, ...]` or `range: [start, end]` to select a
specific subset of lightcurves (otherwise all / `limit`).

### 8. Using a different filter

```bash
BOOM_REPO_ROOT=$(pwd) uv run tests/lsst-simulated/run.py \
    --boom-repo-dir . --config tests/lsst-simulated/example-config.yaml \
    --filter-file /path/to/my_filter.json
```

Loaded into MongoDB by the one-shot `add-filter` container before the
consumer and scheduler start, via the `add_filter` binary built into the boom image.

### 9. Logs

Per-service logs for each run are streamed to `logs/lsst-simulated/<SIM_ID>/`:

```
logs/lsst-simulated/<SIM_ID>/producer.log
logs/lsst-simulated/<SIM_ID>/consumer.log
logs/lsst-simulated/<SIM_ID>/scheduler.log
```

### 10. Teardown

Tear down a single simulation (if you used `--keep-up`):

```bash
BOOM_REPO_ROOT=$(pwd) docker compose \
  -p boom-sim-<SIM_ID> \
  -f tests/lsst-simulated/sim/compose.yaml down -v
```

Tear down the shared infra (stops every simulation sharing it):

```bash
BOOM_REPO_ROOT=$(pwd) docker compose \
  -f tests/lsst-simulated/infra/compose.yaml down -v
```

---

## Inspecting results

All commands below require the stack to be running (`--keep-up`).

### Quick MongoDB summary

```bash
BOOM_REPO_ROOT=$(pwd) bash tests/lsst-simulated/check-mongo.sh
```

Prints counts for ingested alerts, enriched alerts, and filters.

### Inspect alerts in MongoDB

Connect to mongosh:

```bash
BOOM_REPO_ROOT=$(pwd) docker compose -f tests/lsst-simulated/compose.yaml exec mongo \
  mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017/boom-lsst-simulated?authSource=admin"
```

Useful queries inside the shell:

```js
// Count ingested / enriched
db.LSST_alerts.countDocuments()
db.LSST_alerts.countDocuments({ properties: { $exists: true } })

// Inspect one alert (most detections first)
db.LSST_alerts.find().sort({ "candidate.ndethist": -1 }).limit(1)

// Check enrichment properties for one alert
db.LSST_alerts.findOne({}, {
  "properties.star": 1, "properties.stationary": 1,
  "properties.near_brightstar": 1, "properties.rock": 1,
  "properties.photstats": 1,
  "candidate.snr_psf": 1, "candidate.magpsf": 1, "candidate.shape_flag": 1
})

// Alerts with rising/fading computed in band r
db.LSST_alerts.find({ "properties.photstats.r.rising": { $ne: null } })

// All alerts for one object
db.LSST_alerts.find({ objectId: "1000100" })
```

### Check how many alerts passed the filter

```bash
BOOM_REPO_ROOT=$(pwd) docker compose -f tests/lsst-simulated/compose.yaml exec broker \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server broker:29092 --topic LSST_alerts_results \
  | awk -F: '{sum+=$3} END {print "Total passed filter:", sum}'
```

If the topic doesn't exist, 0 alerts passed (Kafka topics are created on first produce).

### Inspect the filter output (candid / objectId)

Requires `confluent-kafka` and `fastavro` (`pip install confluent-kafka fastavro`)
and the broker port exposed (`ports: - "9092:9092"` in `compose.yaml` broker service).

```bash
python3 tests/lsst-simulated/extract_output.py
```

This consumes `LSST_alerts_results` from `localhost:9092` and prints one line per alert:
```
candid=1000100000007  objectId=1000100
```

### Check filter pass rate in scheduler logs

```bash
grep -E "passed|sent total" logs/lsst-simulated/scheduler.log
```

`Successfully sent total of X/Y` — X = sent to Kafka, Y = passed the filter pipeline.
`0/0` means alerts reached the filter but none passed (not that the queue was empty).

### Generate synthetic lightcurves

If your real data has too few detections or all detections on the same night,
generate a synthetic KN-like lightcurve:

```bash
python3 scripts/generate-synthetic-lightcurve.py \
    --output data_alex/lightcurve_LSSTlike_0100.csv \
    --index 100 \
    --t0 63000.0
```

This creates a 10-day lightcurve in g/r/i/z with a rise then fade, producing
enough detections for the KN filter's `nb_data >= 2` rising/fading requirement.
Re-run `run.py` and it will automatically include the new file.
