# LSST Simulated Alert Pipeline — Running on the VM

## Architecture

![Pipeline architecture](boom_architecture.drawio.png)

The input CSV files are read by the Python producer, which converts each observation row into
an LSST Avro alert and publishes it to the Kafka topic `alerts-simulated`. BOOM consumes those
alerts through three worker stages — ingestion, enrichment (cross-match + classifications), and
filtering — storing results in MongoDB. Alerts that pass the filter are published to the Kafka
topic `LSST_alerts_results` and extracted into CSV files for analysis.

---

## Prerequisites — Python environment

Python >= 3.13 and [uv](https://docs.astral.sh/uv/) are required.

### Create the virtual environment

```bash
uv venv                  # creates .venv/ in the repo root
```

### Install dependencies

```bash
uv pip install lsst-alert-packet confluent-kafka numpy pandas fastavro
```

### Activate (optional)

`uv run` resolves the venv automatically, so activation is only needed if you want
to call scripts directly with `python`:

```bash
source .venv/bin/activate      # Linux / macOS
```

> `run.py`, `plot_lightcurves.py`, and `extract_output.py` declare their own dependencies
> via inline metadata and are always invoked with `uv run` — no manual install needed for them.
> Only `produce-lsst-simulated.py` (called via `.venv/bin/python`) requires the venv above.

---

## 1. Connect to the VM

```bash
ssh [login]@157.136.254.119
```

Recommended: add an alias to `~/.bashaliases`:

```bash
alias sshboomvm='ssh [login]@157.136.254.119'
source ~/.bashrc
```

Once connected:

```bash
sudo su
cd /home/almalinux
```

---

## 2. Transfer the dataset to the VM

The Boom repository lives at `/home/almalinux/boom/`. Copy your dataset with:

```bash
rsync -avz --rsync-path="sudo rsync" /path/to/local/dataset/ [login]@157.136.254.119:/home/almalinux/boom/
```

The dataset directory must follow the layout described in `README_input.md`
(`lightcurve_*.csv` + `summary.csv`).

---

## 3. Build the Docker image

Run once, or after any code change:

```bash
BOOM_REPO_ROOT=$(pwd) docker compose -f tests/lsst-simulated/compose.yaml build --no-cache
```

---

## 4. Run the simulation

```bash
BOOM_REPO_ROOT=$(pwd) uv run tests/lsst-simulated/run.py \
    --boom-repo-dir . \
    --data-dir /path/to/dataset \
    --timeout 600 \
    --limit 1000
```

`run.py` will:
1. Generate `tests/lsst-simulated/config.yaml` from the root `config.yaml` (patching DB name, broker address, and credentials for the isolated stack)
2. Count expected alerts from `summary.csv` (or by reading the CSV files directly if `n_detected` is unavailable)
3. Start all services via `tests/lsst-simulated/compose.yaml`
4. Poll MongoDB until all expected alerts are ingested and enriched
5. Tear everything down (unless `--keep-up` is set)

### Options

| Flag | Default | Description |
|---|---|---|
| `--boom-repo-dir` | `.` | Path to the repo root |
| `--data-dir` | `data_alex/` | Directory containing the dataset |
| `--timeout` | `600` | Seconds before the test fails |
| `--limit N` | `0` (no limit) | Process at most N lightcurve files |
| `--keep-up` | off | Keep services running after the test (required to extract results — see step 5) |
| `--filter-file` | `tests/lsst-simulated/filter_KN_PRODUCTION.json` | Filter JSON pipeline to load |

### Using a different filter

```bash
BOOM_REPO_ROOT=$(pwd) uv run tests/lsst-simulated/run.py \
    --boom-repo-dir . \
    --filter-file /path/to/my_filter.json
```

### Stop the simulation manually

```bash
BOOM_REPO_ROOT=$(pwd) docker compose -f tests/lsst-simulated/compose.yaml down -v
```

---

## 5. Extract results (requires `--keep-up`)

When the simulation finishes with `--keep-up`, the Kafka broker is still running and you can
extract filter results into CSV files:

```bash
uv run tests/lsst-simulated/extract_output.py \
    --broker localhost:9092 \
    --data-dir /path/to/dataset \
    --photometry-out photometry.csv \
    --stats-out stats.csv
```

This reads the `LSST_alerts_results` Kafka topic and writes:

| File | Description |
|---|---|
| `photometry.csv` | One row per detection for every object that passed the filter. `passed_filter=True` marks the detection that triggered the pass. Columns: `object_id`, `candid`, `jd`, `mjd`, `band`, `flux_njy`, `flux_err_njy`, `mag`, `mag_err`, `passed_filter` |
| `stats.csv` | One row per passing object with aggregate statistics and filter efficiency. Columns: `object_id`, `target_name`, `ra`, `dec`, `n_detections`, `n_passed_filter`, `filter_names`, `first_jd`, `last_jd` |

`--data-dir` is optional but recommended: it adds the `target_name` field from `summary.csv`
to `stats.csv`.

> Requires `confluent-kafka`, `fastavro`, `lsst-alert-packet` and the broker port exposed
> (`ports: - "9092:9092"` in `compose.yaml` broker service).

After extraction, tear down the stack:

```bash
BOOM_REPO_ROOT=$(pwd) docker compose -f tests/lsst-simulated/compose.yaml down -v
```

---

## 6. Analyse results

Plot lightcurves:

```bash
uv run tests/lsst-simulated/plot_lightcurves.py \
    --photometry photometry.csv \
    --output-dir plots/
```

Check filter pass rate in scheduler logs:

```bash
grep -E "passed|sent total" logs/lsst-simulated/scheduler.log
```

`Successfully sent total of X/Y` — X alerts sent to Kafka, Y passed the filter pipeline.

---

## Logs

While running, per-service logs are streamed to:

```
logs/lsst-simulated/producer.log
logs/lsst-simulated/consumer.log
logs/lsst-simulated/scheduler.log
```

---

## Multi-simulation in parallel

Multiple users can run simulations at the same time on the same VM. The infrastructure
(Kafka broker, MongoDB, Valkey) is shared and runs as a single long-lived stack. Each
simulation gets its own isolated namespace so runs never interfere.

### How the isolation works

The simulation ID (`sim_id`) is generated automatically at launch as:

```
{nickname}_{YYYYMMDD}_{HHMM}      e.g.  cdo_20260819_1430
```

If no nickname is given, it is just `{YYYYMMDD}_{HHMM}`. Every resource in the pipeline
is scoped to this ID:

| Resource | Naming |
|---|---|
| Kafka input topic | `alerts-simulated_{sim_id}` |
| Redis queues | `LSST_alerts_*_queue_{sim_id}` |
| Kafka results topic | `LSST_alerts_results_{sim_id}` |
| MongoDB database | `boom-simu-{sim_id}` |
| Kafka consumer group | `lsst-simulated-{sim_id}` |
| Docker Compose project | `boom-sim-{sim_id}` |
| Per-sim config file | `tests/lsst-simulated/sims/config-{sim_id}.yaml` |
| Logs | `logs/lsst-simulated/{sim_id}/` |

### Start the shared infrastructure (once per VM session)

The broker, MongoDB, and Valkey only need to be started once and stay up across all
simulations and all users:

```bash
BOOM_REPO_ROOT=$(pwd) docker compose -f tests/lsst-simulated/infra/compose.yaml up -d --wait
```

> `run.py` does this automatically and idempotently, so you only need to run this manually
> if you want to pre-warm the infra before launching simulations.

To stop the shared infra (this destroys all simulation databases):

```bash
docker compose -f tests/lsst-simulated/infra/compose.yaml down --volumes
```

### Running simulations in parallel

Each user runs `run.py` with their own nickname from their own terminal:

```bash
# Terminal 1 — user cdo
BOOM_REPO_ROOT=$(pwd) uv run tests/lsst-simulated/run.py \
    --nickname cdo \
    --boom-repo-dir . \
    --data-dir /path/to/dataset_cdo \
    --keep-up

# Terminal 2 — user alexm
BOOM_REPO_ROOT=$(pwd) uv run tests/lsst-simulated/run.py \
    --nickname alexm \
    --boom-repo-dir . \
    --data-dir /path/to/dataset_alexm \
    --keep-up
```

Both simulations share the same Kafka broker, MongoDB server, and Valkey, but each
writes to completely separate topics, queues, and databases.

The `--nickname` flag is optional:

```bash
BOOM_REPO_ROOT=$(pwd) uv run tests/lsst-simulated/run.py --boom-repo-dir . --data-dir /path/to/dataset
# sim_id will be e.g. 20260819_1430
```

### Extracting results from a specific simulation

Pass `--sim-id` to `extract_output.py` to read from that simulation's Kafka topic:

```bash
uv run tests/lsst-simulated/extract_output.py \
    --broker localhost:9092 \
    --sim-id cdo_20260819_1430 \
    --data-dir /path/to/dataset \
    --photometry-out photometry_cdo.csv \
    --stats-out stats_cdo.csv
```

Without `--sim-id`, it reads the default (non-scoped) topic — useful for single-user runs.

### Stopping a specific simulation

```bash
docker compose -p boom-sim-cdo_20260819_1430 -f tests/lsst-simulated/sim/compose.yaml down --volumes
```

### Inspecting a simulation's database

Connect to the shared MongoDB and switch to the simulation's database:

```bash
docker compose -f tests/lsst-simulated/infra/compose.yaml exec mongo \
  mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017/boom-simu-cdo_20260819_1430?authSource=admin"
```

List all simulation databases currently in MongoDB:

```bash
docker compose -f tests/lsst-simulated/infra/compose.yaml exec mongo \
  mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" \
  --quiet \
  --eval "db.adminCommand('listDatabases').databases.map(d => d.name).filter(n => n.startsWith('boom-simu'))"
```

### Logs per simulation

Each simulation streams its logs to its own directory:

```
logs/lsst-simulated/cdo_20260819_1430/producer.log
logs/lsst-simulated/cdo_20260819_1430/consumer.log
logs/lsst-simulated/cdo_20260819_1430/scheduler.log
```

---

## Future steps

- Input files can also handle parquet files
