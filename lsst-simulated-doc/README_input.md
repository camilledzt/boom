# LSST Simulated Alert Pipeline — Input Data Format

## Overview

The producer (`scripts/produce-lsst-simulated.py`) reads per-object lightcurve CSV files
and two index files that map each object to sky coordinates. It converts the photometric
data to LSST Avro alerts (schema v11.0) and publishes them to a Kafka topic.

Two dataset formats are supported. **v2 is the current format.**

---

## Directory layout

```
<data-dir>/
├── lightcurve_<NNNN>.csv      # one file per simulated object (e.g. lightcurve_9994.csv)
├── summary.csv                # maps file index → sky position
└── opsim_fields_index.csv     # maps field_index → RA/Dec  (v1 only)
```

Pass `--data-dir <path>` to the producer to point at any directory with this layout.
The default is `data_alex/` at the repo root.

---

## Lightcurve files — `lightcurve_<NNNN>.csv`

One file per simulated transient. The numeric suffix (e.g. `9994`) is the **file index**
and must match column `i` in `summary.csv`.

### v2 format (current)

All rows are detections. No upper limits, no `detected` flag.

| Column | Type | Description |
|---|---|---|
| `objectid` | int or empty | Object ID — only filled on the **first row**; derived from filename by the script |
| `candidate_id` | string | Per-row identifier used to derive the integer `candid` (e.g. `9994_42`) |
| `time_mjd` | float | Timestamp of the observation (Modified Julian Date) |
| `t_days` | float | Days since event t0 (not used by the pipeline) |
| `band` | string | Photometric filter: `u`, `g`, `r`, `i`, `z`, or `y` |
| `mag` | float | Observed magnitude |

`mag_err` is not present; the pipeline defaults `flux_err` to **5% of flux**.

Example:

```
objectid,candidate_id,time_mjd,t_days,band,mag
9994,9994_1,61405.4959,0.1959,u,16.409
,9994_2,61405.4959,0.1959,g,16.723
,9994_3,61405.4959,0.1959,r,17.067
```

### v1 format (legacy — `data_01_07/`)

Has both detections and upper limits in the same file, distinguished by the `detected` flag.

| Column | Type | Used | Description |
|---|---|---|---|
| `observationStartMJD` | float | yes | Timestamp (MJD) |
| `band` | string | yes | Filter: `u`, `g`, `r`, `i`, `z`, `y` |
| `detected` | bool | yes | `True` = detection; `False` = upper limit |
| `mag_obs` | float or NaN | yes | Observed magnitude (detections only) |
| `mag_err` | float or NaN | yes | Magnitude error (detections only) |
| `mag_ulim` | float or NaN | yes | 5-sigma limiting magnitude (non-detections only) |
| `snr_obs` | float or NaN | yes | Signal-to-noise ratio (optional) |
| `candidate_id` | string | yes | Per-row identifier |
| `objectid` | int | yes | Object ID |
| `m5`, `mag_true`, `snr_exp`, … | various | no | Simulation internals — ignored |

> The v1 format is no longer the target of the current script. See the
> [git history](../scripts/produce-lsst-simulated.py) for the previous implementation
> that handled detections vs upper limits with `diaForcedSource` records.

---

## Flux conversions

All magnitudes are converted to **nano-Jansky (nJy)** using AB zero-point ZP = 31.4.

```
# v2 — detection only
flux      = 10^( -0.4 * (mag - 31.4) )
flux_err  = flux * 0.05                         # 5% default (no mag_err in v2)

# v1 — detection
flux      = 10^( -0.4 * (mag_obs  - 31.4) )
flux_err  = flux * mag_err / (2.5 / ln(10))

# v1 — upper limit (non-detection)
flux_err  = 10^( -0.4 * (mag_ulim - 31.4) ) / 5.0   # 5-sigma → 1-sigma
```

---

## Candidate ID → integer `candid`

`candidate_id` has the form `<index>_<sequence>` (e.g. `9994_42`).

```
object_id = int(filename suffix)         # e.g. 9994 from lightcurve_9994.csv
seq       = int(candidate_id.split("_")[1])
candid    = object_id * 100_000 + seq
```

If the filename suffix is `0` (ambiguous), `object_id` is set to `lc_index + 1_000_000`.

---

## Position index files

### `summary.csv`

Each row corresponds to one lightcurve file. Column `i` is the file index.

**v2 format** — RA/Dec embedded directly:

| Column | Description |
|---|---|
| `i` | File index (matches lightcurve filename suffix) |
| `objectid` | Object ID |
| `fieldRA` | Right ascension (degrees) |
| `fieldDec` | Declination (degrees) |
| *(many others)* | Simulation parameters — ignored |

**v1 format** — requires `opsim_fields_index.csv`:

| Column | Description |
|---|---|
| `i` | File index |
| `field_idx` | Foreign key into `opsim_fields_index.csv` |

The pipeline auto-detects the format: if `fieldRA`/`fieldDec` are present it uses them
directly; otherwise it joins with `opsim_fields_index.csv`.

If `summary.csv` is absent entirely, all objects get `(ra=0, dec=0)`.

### `opsim_fields_index.csv` (v1 only)

| Column | Description |
|---|---|
| `field_index` | Primary key (matched against `field_idx` in `summary.csv`) |
| `ra0_deg` | Right ascension (degrees) |
| `dec0_deg` | Declination (degrees) |

---

## How alerts are built

For each lightcurve file the pipeline:

1. Loads and sorts rows by `time_mjd` (v2) or `observationStartMJD` (v1).
2. For each row, emits one Avro alert containing:
   - the current `diaSource`
   - all previous `diaSource` records as `prvDiaSources`
   - a `diaObject` summary updated with the latest MJD and detection count
3. Serialises the alert with the LSST v11.0 Avro schema and Confluent Schema Registry
   wire format: `[0x00][4-byte schema_id][avro datum]` (schema_id = 1100).

---

## Producer options

```bash
.venv/bin/python scripts/produce-lsst-simulated.py [options]
```

| Flag | Default | Description |
|---|---|---|
| `--broker` | `localhost:9092` | Kafka broker address |
| `--topic` | `alerts-simulated` | Kafka topic to publish to |
| `--data-dir` | `data_alex/` | Directory containing the CSV and index files |
| `--limit N` | `0` (no limit) | Process at most N lightcurve files |
| `--reset` | off | Delete and recreate the Kafka topic before publishing |
| `--workers N` | CPU count | Parallel serialization workers |
