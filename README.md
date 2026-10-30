# tuova_machine_data_etl

> ## IMPORTANT!
>
> This repository was created as part of the TUOVA Project (see https://tuova.savonia.fi and https://tuovaproject.github.io/). Some of the information presented here might assume familiarity with other repositories created during the course of the project. For more details on those, see the "Additional Information" section at the bottom of this README.
>
> **Everything is provided as-is, use at your own risk.**
>
> TUOVA Project co-funded by the European Union / Etelä-Savon ELY-keskus.
>
> ![Co-funded by the EU](co-fundedbythe-eu.png)

Author: Timo Lassila, RDI specialist at Savonia University of Applied Sciences

AI has been used in the development process.

## General

This repository contains an Apache Airflow DAG for an ETL process that turns CNC machine data into machine events:

1. Get raw MTConnect machine data (time counters, overrides, part counter, program) from an InfluxDB (v.2) database (Extract)
2. Classify the machine state minute by minute and derive machine events from it (Transform)
3. Write the events to a PostgreSQL database as per the TUOVA data model (Load)

One run processes one local calendar day (00:00 -> 24:00). The DAG runs daily at 04:00 UTC and processes the previous day.

The derived event types (`event_type_id` in the data model):

| ID  | Event               | Description                                    |
| --- | ------------------- | ---------------------------------------------- |
| 8   | MACHINE PRODUCTION  | Continuous production period of one NC program |
| 10  | MACHINE PART CHANGE | Part counter event with cycle time details     |
| 11  | MACHINE CUTTING     | Period of efficient or inefficient cutting     |
| 14  | MACHINE IDLE        | Machine powered on but not cutting             |
| 16  | POWER OFF           | Machine powered off                            |
| 17  | MACHINE DAY         | Daily summary                                  |

The events are written with `source_id` = `2` (MACHINE API), which separates them from other sources writing to the same table (e.g. sensor data).

In addition, the raw state and part rows are written into daily CSV files. These are meant for debugging only.

How each step works, and the concepts needed to follow it, is explained in [How the ETL works](docs/how-it-works.md).

## Prerequisites

- It is assumed that an InfluxDB database with MTConnect machine data is already deployed. The measurement and item names are set in the configuration (`signals`); the defaults are those of the TUOVA collector (measurements `mtc_metrics` and `mtc_status`)
- It is also assumed that a PostgreSQL database with the data model schema is already deployed. **The DAG does not create the target table** (`fact_machine_events`), it only checks that the table and its columns exist.
- Airflow 3 is assumed to be deployed with the official `docker-compose` setup (tested with the standard image `apache/airflow:3.0.4`, no custom build)
- For the local test environment: Docker with Docker Compose, and a bash shell

## Development setup

The repository includes a local test environment that needs no external services or VPN. It contains InfluxDB (source), PostgreSQL (target) and Airflow running the same image as production.

```bash
dev/setup.sh       # start containers, create config files and the DAG's virtual environment
dev/seed.sh        # load synthetic machine data for yesterday into InfluxDB
dev/test.sh        # check that the DAG parses and run the unit and regression tests
dev/run-day.sh     # run one day through the DAG and show the result per event type
```

`dev/seed.sh` and `dev/run-day.sh` also accept a date, e.g. `dev/seed.sh 2026-10-06 3` loads three days of data starting from the given date.

The regression test runs two synthetic days through the whole calculation and compares the result to the files in `tests/odotetut/`. If a change to the result is intentional, rewrite the files with `dev/test.sh paivita` and review the change with `git diff`. The synthetic data comes from `tests/synteettinen_data.py`, the same generator that `dev/seed.sh` uses.

Airflow UI: http://localhost:8082 (`airflow` / `airflow`). Ports can be changed in `.env`. `dev/setup.sh down` stops the environment and deletes its data.

Note that the test database has no foreign keys or dimension tables. A value missing from e.g. `dim_workcenter` will only show up in production.

## Configuration

All of the DAG's own files live in one directory, `config/python-venvs/mtconnect_data_dag/`, which is mounted into the containers as `/opt/airflow/config/python-venvs/mtconnect_data_dag/`. The configuration is split into two files:

### Credentials: `.env`

Create as per `.env.example`. Not to be committed.

- `INFLUX_URL`, `INFLUX_TOKEN`, `INFLUX_ORG`, `INFLUX_BUCKET`: Your InfluxDB details
- `PG_DSN`: PostgreSQL connection string to the target database
- `PG_DSN_FALLBACK`: (optional) Used only when `PG_DSN` is missing, with a warning in the log

### Settings: `mtconnect_data_config.yaml`

Create as per `mtconnect_data_config.example.yaml`, where every setting is explained in detail. The file is read at the start of every run, so changes apply to the next run without restarting Airflow. Missing keys fall back to defaults, and the task log tells which values came from the file. The sections:

- `connection`: SSL verification and query timeout
- `target`: Target table, write mode and `source_id`
- `machines`: The machine to process: `workcenter_id` and optionally `device`
- `time`: Clock error, timezone and how much data is fetched around the target day
- `source`: Which InfluxDB tags name the measured item and the device
- `signals`: Where each signal used by the calculation is found in InfluxDB
- `calculation`: Thresholds for state classification and cycle time limits
- `files`: Location of the debug CSV files

At least the following should be checked before the first run:

- `machines`: **Required.** The machine's `workcenter_id` in `dim_workcenter`. If the bucket holds data from several machines, also set `device` (the value of the device tag in InfluxDB) so that only this machine's data is fetched. For now one installation processes one machine.
- `time.clock_error_hours`: How many hours the InfluxDB timestamps lag behind UTC. The default is `0` (timestamps are correct).
- `time.timezone`: Local time (e.g. `Europe/Helsinki`). Determines the day boundaries and the values of `start_date`, `end_date`, `start_time` and `end_time`. Daylight saving time is taken into account automatically.
- `signals`: The names your collector uses. The defaults match the TUOVA collector. With another collector or controller, change only the `measurement` and `name` of each signal, and the `unit` of the time counters (`ms`, `s`, `min` or `h`). The InfluxDB queries are derived from these.
- `target.write_mode`: `replace_day` (default) deletes the day's rows of this DAG and writes them again, so a day can be re-run safely. `append` only inserts and will create duplicates.

### Signals

The calculation uses logical signal names, not the collector's names. A missing required signal stops the run and names both the signal and the source name it looked for.

| Signal             | Kind         | Required | Default (TUOVA collector)         |
| ------------------ | ------------ | -------- | --------------------------------- |
| `running_time`     | time counter | yes      | `mtc_metrics/ptotalrunningtime`    |
| `operating_time`   | time counter | yes      | `mtc_metrics/ptotaloperatingtime`  |
| `cutting_time`     | time counter | yes      | `mtc_metrics/ptotalcuttingtime`    |
| `spindle_time`     | time counter | yes      | `mtc_metrics/ptotalspindleruntime` |
| `feed_override`    | set value    | yes      | `mtc_metrics/pfovr`                |
| `spindle_override` | set value    | no       | `mtc_metrics/s1ovr`                |
| `program`          | set value    | yes      | `mtc_status/pprogram`              |
| `part_count`       | part counter | yes      | `mtc_metrics/ppartcount`           |

Time counters are cumulative and fetched as per-minute maximums. Set values are written only when they change, so they also get a warm-up query for the last value before the day. The part counter is fetched as is. More: [How the queries are derived](docs/how-it-works.md#how-the-queries-are-derived).

## Deployment on Linux OS

### General

The DAG uses Airflow's `@task.external_python` to run all calculations in a separate virtual environment. This keeps the DAG's dependencies (pandas, influxdb-client, psycopg2) away from Airflow's own, and the standard Airflow image can be used as is.

For the same reason, **the DAG file must only import Airflow and the Python standard library**. Any other import makes the DAG fail to parse in production.

### The virtual environment

1. Copy `config/python-venvs/mtconnect_data_dag/` to Airflow's `config` folder on the host machine (without `venv/` and `data/` if they exist)
2. In that directory, create `.env` and `mtconnect_data_config.yaml` as per the [Configuration](#configuration) section above
3. Create the virtual environment inside the worker container (adjust the container name as needed):

```bash
docker exec airflow-worker python -m venv \
  /opt/airflow/config/python-venvs/mtconnect_data_dag/venv
docker exec airflow-worker \
  /opt/airflow/config/python-venvs/mtconnect_data_dag/venv/bin/pip install \
  -r /opt/airflow/config/python-venvs/mtconnect_data_dag/requirements.txt
```

Do not copy the virtual environment from another machine. It is linked to the container's Python interpreter and breaks if the version differs.

### The DAG

4. Copy the DAG file (`dags/mtconnect_data_dag.py`) to Airflow's `dags` folder
5. Check that the DAG parses (`airflow dags list-import-errors` in the container)
6. Unpause the DAG in the Airflow web GUI. It is paused by default.

A single day can be run manually with the `Trigger` button, where the parameter "Ajopäivä" selects the day. Date ranges can be run with Airflow's backfill.

Pro Tip: if a run fails, its intermediate files are kept under `data/staging/` in the DAG directory for troubleshooting. After a successful run they are removed.

## Notes

The concepts, the steps of a run and the code modules are explained in [How the ETL works](docs/how-it-works.md). More detailed technical documentation (time handling, task flow, re-runs) can be found in the docstring of `dags/mtconnect_data_dag.py`, which is also shown in the Airflow GUI.

## LICENSE

See [LICENSE](LICENSE)

## Additional Information

### The TUOVA Project

- https://tuova.savonia.fi
- https://tuovaproject.github.io/

### The data model ("Tuotantotiedon tietomalli")

- https://github.com/TUOVAProject/tuotantotiedon-tietomalli
- https://tuovaproject.github.io/tuotantotiedon-tietomalli/

### On-premises server environment

- https://github.com/TUOVAProject/tuova_on_prem_server

### Sensor data collection system

- https://github.com/TUOVAProject/tuova-iot

### Energy analyzer data collection system

- https://github.com/TUOVAProject/tuova_energymeter

### Sensor data ETL process

- https://github.com/TUOVAProject/tuova-sensor-etl

### Sensor data - further processing

- https://github.com/TUOVAProject/tuova_machinedata_processing

### Machine MTConnect data collector

- https://github.com/TUOVAProject/tuova_machine_data_collector