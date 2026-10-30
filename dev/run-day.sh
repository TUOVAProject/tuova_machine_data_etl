#!/usr/bin/env bash
#
# Yksi vuorokausi DAGin läpi testiympäristössä, ja tulos taskeittain sekä kohdetaulusta.
#
#   dev/run-day.sh                eilinen vuorokausi (ladattava ensin: dev/seed.sh)
#   dev/run-day.sh 2026-10-06     annettu vuorokausi
#
# Ajo käynnistetään triggerillä eikä `airflow dags test` -komennolla: jälkimmäinen
# kilpailee schedulerin kanssa samoista taskeista. DAG otetaan pois pausesta, jolloin
# scheduler voi ajaa myös oman ajastetun ajonsa (eilinen vuorokausi).
#
# Onnistuminen luetaan TASKIEN tiloista: DagRun voi näkyä onnistuneena vaikka
# fail_fast olisi ohittanut taskeja.
#
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PAIVA="${1:-$(eilen)}"
RUN_ID="testi_${PAIVA}_$(date -u +%Y%m%dT%H%M%S)"
ODOTUS_S=1800                     # DAGin dagrun_timeout on 25 min
VALI_S=10
# Tilat joissa taski ei ole vielä valmis
KESKEN='(none|queued|scheduled|running|up_for_retry|up_for_reschedule|deferred|restarting)='

# Taskien tilat muodossa "success=12 failed=1". JSON jäsennetään kontin pythonilla.
taskien_tilat() {
    in_scheduler bash -c "airflow tasks states-for-dag-run '$DAG_ID' '$RUN_ID' -o json 2>/dev/null \
        | python -c 'import collections, json, sys
rows = json.load(sys.stdin)
counts = collections.Counter((row.get(\"state\") or \"none\") for row in rows)
print(\" \".join(f\"{state}={n}\" for state, n in sorted(counts.items())))'" 2>/dev/null || true
}

step "DAG pois pausesta"
in_scheduler airflow dags unpause "$DAG_ID" >/dev/null
echo "OK"

step "Käynnistys: $RUN_ID (target_date $PAIVA)"
in_scheduler airflow dags trigger "$DAG_ID" --run-id "$RUN_ID" \
    -c "{\"target_date\": \"$PAIVA\"}" >/dev/null
echo "Seuranta myös UI:ssa: http://localhost:${TEST_AIRFLOW_PORT:-8082}/dags/$DAG_ID"

step "Odotetaan taskeja (enintään $((ODOTUS_S / 60)) min)"
loppu=$((SECONDS + ODOTUS_S))
tilat=""
while (( SECONDS < loppu )); do
    tilat="$(taskien_tilat)"
    if [[ -n "$tilat" ]] && ! grep -qE "(^| )$KESKEN" <<<"$tilat"; then
        break
    fi
    printf '\r  %s  ' "${tilat:-odottaa schedulerin käsittelyä}"
    sleep "$VALI_S"
done
echo
echo "Taskit: $tilat"

step "Kohdetaulu: päivän $PAIVA rivit tapahtumatyypeittäin"
pvm="${PAIVA//-/}"
warehouse_sql "select event_type_id, count(*) as riveja
               from public.fact_machine_events
               where $pvm in (start_date, end_date)
               group by 1 order by 1"

if [[ "$tilat" =~ ^(success=[0-9]+)$ ]]; then
    echo "OK: kaikki taskit onnistuivat"
else
    echo "VIRHE: kaikki taskit eivät onnistuneet. Lokit: logs/dag_id=$DAG_ID/run_id=$RUN_ID/"
    exit 1
fi
