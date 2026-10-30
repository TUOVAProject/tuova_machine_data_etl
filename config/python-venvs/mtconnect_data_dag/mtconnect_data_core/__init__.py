"""
mtconnect_data_core -- mtconnect_data_dagn laskenta InfluxDB:stä PostgreSQL:ään.

Tämä paketti ajetaan ULKOISESSA virtuaaliympäristössä, ei Airflow'n tulkissa. Siksi
täällä ei ole yhtään Airflow-importtia: DAG-tiedosto mtconnect_data_dag.py on ohut kääre,
joka kutsuu tämän paketin funktioita @task.external_python -taskeista.

Työnjako
    mtconnect_data_dag.py        Airflow: aikataulu, taskit, riippuvuudet. Pelkät
                            Airflow-importit, jotta DAG jäsentyy vakioimagessa.
    mtconnect_data_core/     kaikki laskenta ja I/O. Vaatii pandasin, influxdb-clientin
                            ja psycopg2:n, jotka ovat venvissä.

Miksi näin: tuotanto ajaa vakioimagea apache/airflow:3.0.4, jossa ei ole
influxdb-clientiä. Jos DAG importtaisi sen moduulitasolla, se ei jäsentyisi lainkaan.

PAKETIN RAKENNE
    Moduulit riippuvuusjärjestyksessä -- ylempi ei koskaan tuo alempaa:

        vakiot.py           signaalit, laskennan vakiot, kohdetaulun sarakkeet, oletusasetukset
        apurit.py           polut, aikaikkunat ja aikamuunnokset, kone, .env, loki
        asetukset.py        asetustiedoston luku, oletukset ja tarkistukset
        influx.py           hakujen suunnittelu, Flux-kysely, käännös signaaleiksi
        raakataso.py        tilajaksot, kappaletapahtumat, CSV ("data v1")
        konetapahtumat.py   tapahtumatyypit 8, 10, 11, 14, 16 ja 17
        postgres.py         kohdetaulun tarkistus ja kirjoitus
        vaiheet.py          DAGin taskien kutsumat funktiot

    Tämä __init__.py ei sisällä logiikkaa vaan kokoaa julkisen rajapinnan: DAG kutsuu
    core.load_config(), core.build_machine_events() ja niin edelleen tietämättä mistä
    moduulista kukin tulee.

AJOKONTEKSTI
    Airflow'n get_current_context() ei toimi täällä. DAG-puoli poimii tarvittavat
    arvot ja välittää ne tavallisena sanakirjana, jota kutsutaan nimellä "ctx":

        run_id               ajon tunnus, staging-hakemiston nimeämiseen
        data_interval_start  ISO-merkkijonona (puuttuu, jos ajolla ei ole intervallia)
        data_interval_end    ISO-merkkijonona (puuttuu, jos ajolla ei ole intervallia)
        target_date          laskettava päivä YYYY-MM-DD, paikallinen kalenteripäivä

    Sama sanakirja kulkee joka funktiolle, joten koko ajo käyttää samoja arvoja.

TUNNUKSET
    Venvissä ei ole Airflow'ta, joten Connectioneja ei voi lukea. Tunnukset luetaan
    .env-tiedostosta paketin ISÄNTÄhakemistosta (venv/:n rinnalta):

        INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
        PG_DSN              kohdekanta
        PG_DSN_FALLBACK     valinnainen varakanta, käytetään jos PG_DSN puuttuu

    Arvot eivät saa päätyä lokiin. Tiedoston oikeudet on syytä rajata (chmod 600).

SIGNAALIT
    Laskenta käyttää signaalien loogisia nimiä (vakiot.SIGNALS), ei keruun nimiä.
    Asetusten signals-osio kertoo, millä measurementilla ja nimellä kukin löytyy
    Influxista; haut johdetaan siitä, ja combine_and_validate_raw_data kääntää
    lähteen nimet loogisiksi. Toinen keruu tai ohjain on siis pelkkä asetusmuutos.

Muu dokumentaatio -- aikaleimojen korjaus, tulkinnat, tapahtumatyypit -- on
DAG-tiedoston mtconnect_data_dag.py docstringissä, joka näkyy myös Airflow'n UI:ssa.
"""

# Julkinen rajapinta: DAGin taskien kutsumat vaiheet, samassa järjestyksessä kuin ne
# ajossa suoritetaan. DAG-tiedostoa ei tarvinnut muuttaa jaon yhteydessä, koska nämä
# nimet säilyivät ennallaan.
from .vaiheet import (
    load_config,
    plan_influx_queries_for_interval,
    fetch_influx_query_to_staging,
    combine_and_validate_raw_data,
    classify_machine_state_segments,
    compute_part_cycle_events,
    build_raw_event_rows,
    write_daily_event_partition,
    merge_partitions_to_combined_csv,
    build_machine_events,
    load_to_postgres,
    cleanup_run_staging_files,
)

# Vakiot ja polut, joita testit ja ylläpitoskriptit lukevat suoraan.
from .vakiot import (
    CONFIG_FILE,
    DEFAULT_CONFIG,
    ENV_FILE,
    EVENT_COLUMNS,
    MACHINE_EVENT_COLUMNS,
    SIGNALS,
    TARGET_COLUMN_MAP,
    TARGET_COLUMNS,
    VALID_WRITE_MODES,
)

__all__ = [
    "load_config",
    "plan_influx_queries_for_interval",
    "fetch_influx_query_to_staging",
    "combine_and_validate_raw_data",
    "classify_machine_state_segments",
    "compute_part_cycle_events",
    "build_raw_event_rows",
    "write_daily_event_partition",
    "merge_partitions_to_combined_csv",
    "build_machine_events",
    "load_to_postgres",
    "cleanup_run_staging_files",
    "CONFIG_FILE",
    "DEFAULT_CONFIG",
    "ENV_FILE",
    "EVENT_COLUMNS",
    "MACHINE_EVENT_COLUMNS",
    "SIGNALS",
    "TARGET_COLUMN_MAP",
    "TARGET_COLUMNS",
    "VALID_WRITE_MODES",
]
