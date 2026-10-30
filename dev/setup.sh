#!/usr/bin/env bash
#
# Käynnistää testiympäristön ja valmistelee DAGin ajokuntoon:
#
#   1. .env (compose) ja DAGin asetustiedosto esimerkeistä
#   2. kontit käyntiin ja terveiksi
#   3. DAGin .env testiarvoista (dev/test.env)
#   4. DAGin venv worker-kontin sisällä requirements.txt:stä
#
# Turvallinen ajaa uudelleen: olemassa oleviin tiedostoihin ei kosketa, ja venv
# päivitetään vain jos requirements.txt on muuttunut.
#
#   dev/setup.sh
#   dev/setup.sh down     pysäyttää ympäristön ja poistaa sen datan (volumet)
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if [[ "${1:-}" == "down" ]]; then
    "${COMPOSE[@]}" down -v
    exit 0
fi

# Kopioi esimerkin, jos kohdetta ei vielä ole. Olemassa olevaa ei ylikirjoiteta.
luo_esimerkista() {
    local esimerkki="$1" kohde="$2"
    if [[ -e "$kohde" ]]; then
        echo "on jo:  ${kohde#"$REPO_DIR"/}"
    else
        cp "$esimerkki" "$kohde"
        echo "luotu:  ${kohde#"$REPO_DIR"/}"
    fi
}

step "1/4 Asetustiedostot"
if [[ ! -e "$REPO_DIR/.env" ]]; then
    sed "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" "$REPO_DIR/.env.example" > "$REPO_DIR/.env"
    echo "luotu:  .env (AIRFLOW_UID=$(id -u))"
else
    echo "on jo:  .env"
fi
luo_esimerkista "$VENV_DIR_HOST/mtconnect_data_config.example.yaml" "$VENV_DIR_HOST/mtconnect_data_config.yaml"
# Mountattavat hakemistot omiksi ennen kuin Docker luo ne rootin omistukseen
mkdir -p "$REPO_DIR/logs" "$REPO_DIR/plugins"

step "2/4 Kontit käyntiin (ensimmäisellä kerralla imaget latautuvat, kestää minuutteja)"
"${COMPOSE[@]}" up -d --wait

step "3/4 DAGin tunnukset testiarvoista"
# Avaimet .env.examplesta (myös kommentoidut), arvot influxdb-kontin ympäristöstä,
# johon compose on jo lukenut dev/test.env:n viittauksineen.
DAG_ENV="$VENV_DIR_HOST/.env"
if [[ -e "$DAG_ENV" ]]; then
    echo "on jo:  ${DAG_ENV#"$REPO_DIR"/}"
else
    {
        echo "# Testiympäristö: luotu dev/setup.sh:lla dev/test.env:n arvoista."
        for avain in $(grep -oE '^#?[A-Z_]+=' "$VENV_DIR_HOST/.env.example" | tr -d '#='); do
            if arvo="$(in_influxdb printenv "$avain")"; then
                echo "$avain=$arvo"
            fi
        done
    } > "$DAG_ENV"
    chmod 600 "$DAG_ENV"
    echo "luotu:  ${DAG_ENV#"$REPO_DIR"/} ($(grep -c '=' "$DAG_ENV") avainta)"
fi

step "4/4 DAGin venv worker-kontissa"
if [[ ! -x "$VENV_DIR_HOST/venv/bin/python" ]]; then
    in_worker python -m venv "$VENV_DIR/venv"
    echo "luotu:  venv"
fi
in_worker "$VENV_PY" -m pip install --quiet --disable-pip-version-check -r "$VENV_DIR/requirements.txt"
echo "paketit asennettu"

step "Valmis"
echo "Airflow UI:  http://localhost:${TEST_AIRFLOW_PORT:-8082}  (airflow / airflow)"
echo "Seuraavaksi: dev/seed.sh  ->  dev/test.sh  ->  dev/run-day.sh"
