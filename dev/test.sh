#!/usr/bin/env bash
#
# Kohdennetut tarkistukset testiympäristössä (vaatii dev/setup.sh:n):
#
#   1. DAG jäsentyy tuotannon vakioimagessa (ei import-virheitä)
#   2. yksikkö- ja regressiotestit venvin tulkilla worker-kontissa
#
#   dev/test.sh            tarkistukset
#   dev/test.sh paivita    kirjoittaa regressiotestin odotetut tiedostot uudelleen
#                          (tests/odotetut/). Käytä vain kun tuloksen muutos on
#                          tarkoituksellinen, ja tarkista muutos: git diff
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PAIVITA=0
[[ "${1:-}" == "paivita" ]] && PAIVITA=1

step "1/2 DAG jäsentyy"
virheet="$(in_scheduler airflow dags list-import-errors -o plain 2>/dev/null || true)"
if grep -q "$DAG_ID" <<<"$virheet"; then
    echo "$virheet"
    exit 1
fi
# Pelkkä virheettömyys ei riitä: DAGin on myös löydyttävä
if ! in_scheduler airflow dags list -o plain 2>/dev/null | grep -qw "$DAG_ID"; then
    echo "DAGia $DAG_ID ei löydy. Jäsentäminen voi kestää hetken käynnistyksen jälkeen."
    exit 1
fi
echo "OK: $DAG_ID löytyy, ei import-virheitä"

step "2/2 Yksikkö- ja regressiotestit"
in_worker env PAIVITA_ODOTETUT="$PAIVITA" "$VENV_PY" -m unittest discover -s "$VENV_DIR/tests" -v
if (( PAIVITA )); then
    echo "Odotetut tiedostot kirjoitettu uudelleen. Tarkista muutos: git diff -- '*/tests/odotetut/'"
fi
