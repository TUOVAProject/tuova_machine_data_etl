#!/usr/bin/env bash
#
# Synteettinen konedata testiympäristön InfluxDB:hen (tests/synteettinen_data.py DAGin
# hakemistossa; sama generaattori tuottaa regressiotestin syötteen).
#
#   dev/seed.sh                   eilinen vuorokausi, sama jonka ajastettu ajo laskisi
#   dev/seed.sh 2026-10-06        annettu vuorokausi
#   dev/seed.sh 2026-10-06 3      kolme vuorokautta alkaen annetusta
#
# Generaattori ajetaan worker-kontissa DAGin venvillä ja se lukee DAGin asetukset,
# joten kentät ja kellovirhe ovat samat kuin DAGilla. Turvallinen ajaa uudelleen:
# samat pisteet korvaavat itsensä. Line protocol jää talteen dev/testdata/generated/.
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PAIVA="${1:-$(eilen)}"
VRK="${2:-1}"
TIEDOSTO="$GENERATED_DIR/machine-data_${PAIVA}_${VRK}d.lp"
mkdir -p "$GENERATED_DIR"

step "Generoidaan $PAIVA + $VRK vrk"
in_worker "$VENV_PY" "$VENV_DIR/tests/synteettinen_data.py" --date "$PAIVA" --days "$VRK" \
    > "$TIEDOSTO"

step "Ladataan InfluxDB:hen"
in_influxdb sh -c 'influx write --bucket "$INFLUX_BUCKET" --precision s' < "$TIEDOSTO"
echo "Ladattu $(wc -l < "$TIEDOSTO") pistettä (${TIEDOSTO#"$REPO_DIR"/})"
