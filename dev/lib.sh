# Yhteiset määrittelyt dev/-skripteille. Ladataan:  source "$(dirname "$0")/lib.sh"
#
# Polut ja nimet ovat tässä kerran. DAG-tiedosto käyttää samoja kontin polkuja
# (VENV_DIR mtconnect_data_dag.py:ssä).

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "$REPO_DIR/docker-compose.yaml")

DAG_ID="mtconnect_data_dag"

# DAGin hakemisto kontin sisällä ja hostilla (sama hakemisto, config/ on mountattu)
VENV_DIR="/opt/airflow/config/python-venvs/$DAG_ID"
VENV_PY="$VENV_DIR/venv/bin/python"
VENV_DIR_HOST="$REPO_DIR/config/python-venvs/$DAG_ID"

# Generoitu line protocol talteen (generaattori: tests/synteettinen_data.py DAGin hakemistossa)
GENERATED_DIR="$REPO_DIR/dev/testdata/generated"

step() { printf '\n==> %s\n' "$*"; }

# Ajastetun ajon tavoitepäivä: data_interval_startin UTC-päivä eli eilinen.
eilen() { date -u -d yesterday +%F; }

# Komennot kontteihin. -T: ei TTY:tä, jotta putket ja ohjaukset toimivat.
in_worker()    { "${COMPOSE[@]}" exec -T airflow-worker "$@"; }
in_scheduler() { "${COMPOSE[@]}" exec -T airflow-scheduler "$@"; }
in_influxdb()  { "${COMPOSE[@]}" exec -T influxdb "$@"; }
in_warehouse() { "${COMPOSE[@]}" exec -T warehouse "$@"; }

# SQL tietovarastoon testitunnuksilla (dev/test.env kontin ympäristössä).
warehouse_sql() {
    in_warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "$1"' _ "$1"
}
