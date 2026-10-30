"""
mtconnect_data_dag.py
mtconnect_data_dag: konedatan tapahtumamalli InfluxDB:stä PostgreSQL:ään.

Laskenta ajetaan ULKOISESSA virtuaaliympäristössä. Tämä tiedosto on ohut
kääre: kaikki laskenta on mtconnect_data_core-paketissa, jonka
@task.external_python -taskit importtaavat.

MIKSI ULKOINEN VENV
    Tuotanto ajaa vakioimagea apache/airflow:3.0.4 ilman build-vaihetta, eikä siinä
    ole influxdb-clientiä. Jos DAG importtaisi sen moduulitasolla, DAG ei jäsentyisi
    lainkaan vaan näkyisi Airflow'ssa import-virheenä.

    Siksi tässä tiedostossa EI SAA OLLA muita kuin Airflow- ja stdlib-importteja.
    pandas, influxdb_client, psycopg2 ja yaml importataan vasta venvin puolella.
    Jos lisäät tänne kolmannen osapuolen importin, DAG lakkaa toimimasta
    tuotannossa -- eikä se näy kehityksessä, jossa kirjastot ovat imagessa.

HAKEMISTO
    Kaikki DAGin oma tavara on yhdessä paikassa, joka on valmiiksi mountattu
    molemmissa ympäristöissä (tuotanto mounttaa vain dags/logs/config/plugins):

        /opt/airflow/config/python-venvs/mtconnect_data_dag/
            venv/                       ulkoinen virtuaaliympäristö
            requirements.txt            venvin sisältö
            mtconnect_data_core/         laskenta, jaettuna moduuleihin:
                __init__.py                 julkinen rajapinta (vaiheet)
                vakiot.py                   signaalit, vakiot ja oletusasetukset
                apurit.py                   polut, aikaikkuna, kone, .env, loki
                asetukset.py                asetusten luku ja tarkistukset
                influx.py                   haut, Flux-kysely, signaaleiksi
                raakataso.py                tilajaksot ja kappaletapahtumat
                konetapahtumat.py           tyypit 8, 10, 11, 14, 16, 17
                postgres.py                 kohdetaulu ja kirjoitus
                vaiheet.py                  taskien kutsumat funktiot
            mtconnect_data_config.yaml   asetukset
            .env                        tunnukset (ei versionhallintaan)
            data/output, data/staging   CSV-partitiot ja välidata

    Moduulit ovat riippuvuusjärjestyksessä: ylempi ei koskaan tuo alempaa. Taskit
    kutsuvat vain paketin julkista rajapintaa, joten sisäinen jako voi muuttua
    koskematta tähän tiedostoon.

    Venviä ei kopioida ympäristöjen välillä vaan se luodaan kummassakin erikseen
    requirements.txt:stä -- ks. ohje kyseisestä tiedostosta.

KAKSI TASOA
    1. Raakataso ("data v1")  -- tilamuutos- ja kappalerivit, kirjoitetaan CSV:ksi.
    2. Konetapahtumat          -- kuusi johdettua tapahtumatyyppiä raakatasosta:
           8  MACHINE PRODUCTION   saman ohjelman yhtenäinen tuotantojakso
           10 MACHINE PART CHANGE  kappalelaskuritapahtuma jaksotietoineen
           11 MACHINE CUTTING      yksi tehokkaan/tehottoman työstön jakso
           14 MACHINE IDLE         kone päällä mutta ei työstössä
           16 POWER OFF            sammutusjakso
           17 MACHINE DAY          vuorokauden koonti, suoraan raakadatasta

SIGNAALIT
    Laskenta käyttää signaalien loogisia nimiä (running_time, feed_override,
    program ...), ei keruun nimiä. Asetusten signals-osio kertoo, millä
    measurementilla ja nimellä kukin signaali löytyy Influxista:

        signals.program: {measurement: mtc_status, name: pprogram}

    Haut johdetaan tästä (yksi haku per hakutapa ja measurement), ja
    combine_and_validate_raw_data kääntää lähteen nimet loogisiksi ja laskurit
    sekunneiksi. Toinen keruu tai ohjain on siis pelkkä asetusmuutos.

    Haettava kone on asetusten machines-listassa (toistaiseksi yksi kone per
    asennus). Jos koneelle on annettu device, haku rajataan siihen laitteeseen.

AJON KULKU
    Yksi ajo = yksi vuorokausi. Kaikki taskit paitsi resolve_run_context ajetaan
    venvissä. Taskit siirtävät dataa toisilleen levyn kautta: XComissa kulkevat vain
    tiedostopolut ja pienet sanakirjat, ei DataFrameja.

        resolve_run_context             ajon konteksti sanakirjaksi (Airflow)
        load_config                     asetukset mtconnect_data_config.yaml:sta
             |
        plan_influx_queries_for_interval  haut signaaleista, aikaväli
             |
        fetch_influx_query_to_staging   yksi task per kysely (dynaaminen .expand)
             |                          -> staging/<run_id>/raw_<key>.pkl
        combine_and_validate_raw_data   yhdistys + signaalit + aikaleimat + tarkistus
             |                          -> combined.pkl
             +-----------------------------+
             |                             |
        classify_machine_       compute_part_cycle_events
        state_segments          (kappalelaskurin vaihtumishetket)
        (tilajaksot)                       |
             +-----------------------------+
             |
        build_raw_event_rows            raakataso, rajattu tavoitevuorokauteen
             |                          -> raw_events.pkl
             +-----------------------------+
             |                             |
        write_daily_event_          build_machine_events   kuusi tapahtumatyyppiä
        partition (CSV, debug)             |
             |                      load_to_postgres      kirjoitus kohdetauluun
        merge_partitions_                  |
        to_combined_csv                    |
             +-----------------------------+
             |
        cleanup_run_staging_files       välidata pois onnistuneen ajon jälkeen

AJOKONTEKSTI
    get_current_context() ei toimi venvissä, koska siellä ei ole Airflow'ta.
    resolve_run_context poimii tarvittavat arvot Airflow-puolella ja välittää ne
    tavallisena sanakirjana ("ctx") joka taskille. Sanakirja on sama koko ajon
    ajan, joten kaikki vaiheet käyttävät samoja arvoja.

AIKALEIMOJEN KORJAUS
    Kellonaika muodostuu kahdesta erillisestä asiasta, joilla on omat asetuksensa:

        clock_error_hours  KELLOVIRHE. InfluxDB:n aikaleimat ovat tämän verran
                           jäljessä UTC:stä (TUOVA-ympäristössä 6 h, oletus 0).
                           Vakio ympäri vuoden.
        timezone           PAIKALLINEN AIKA (Europe/Helsinki). Kesä- ja talviaika
                           tulevat aikavyöhyketietokannasta, ei käsin.

        Influx-aika  --(+ kellovirhe)-->  oikea UTC  --(timezone)-->  paikallinen aika
                                          LASKENTA                    vuorokauden rajat,
                                                                      ulos kirjoitettavat kentät

    TUOVA-ympäristössä korjaus on yhteensä kesällä 9 h ja talvella 8 h. Aiemmin
    käytössä oli yksi kiinteä luku (time_offset_hours: 9), joka oli oikein vain
    kesäaikana.

    Laskenta tehdään UTC:ssä, koska siinä ei ole kellonsiirtoja: kestot ja
    järjestys pysyvät oikeina myös päivänä, jona sama paikallinen tunti toistuu.
    Paikalliseen aikaan muunnetaan vain reunoilla.

    Kellovirhe korjataan kahdessa kohdassa, ja molempia tarvitaan:

        1. Hakuikkuna  plan_influx_queries_for_interval VÄHENTÄÄ kellovirheen
                       hakurajoista, koska Influxia on kysyttävä sen omalla
                       virheellisellä aikajanalla.
        2. Aikaleimat  combine_and_validate_raw_data LISÄÄ kellovirheen dataan,
                       jolloin tulos on oikeassa UTC-ajassa.

    Pelkkä kohta 2 korjaa esityksen muttei ikkunaa: partitio ei kattaisi
    paikallista vuorokautta 00:00 -> 24:00, jota määrittelyn kohta 9 edellyttää.

    Kohdetaulun aikakentät:

        start_at, end_at        oikea hetki (timestamptz). Kirjoitetaan UTC-erotuksen
                                kanssa, joten kannan TimeZone-asetus ei vaikuta.
        start_date, end_date    paikallinen päivä YYYYMMDD
        start_time, end_time    paikallinen kellonaika HHMMSS

    Kaikki aikamuunnokset ovat yhdessä paikassa: mtconnect_data_core/apurit.py.

TULKINNAT
    Määrittely viittaa kolmessa kohdassa Excel-työkirjaan, jota ei ole
    käytettävissä. Nämä on merkitty core-moduuliin kommentilla "TULKINTA" ja ne on
    vahvistettava työkirjaa vasten ennen kuin tuloksiin voi luottaa:
        1. Tyypin 10 start_at / stop_at johtaminen
        2. Tyypin 8 jaksotus -- mikä on "selkeä tuotantokatkos"
        3. Tyypin 10 idle_sum_min -- suhde part_change_time_min:iin

AIKAIKKUNA
    Tavoite:   paikallinen kalenteripäivä 00:00 -> 24:00. Yleensä 24 h,
               kellonsiirtopäivinä 23 h (maaliskuu) tai 25 h (lokakuu).
    Haku:      [tavoite_alku - kellovirhe - lookback,
                tavoite_loppu - kellovirhe + lookahead)
               Influxin omalla aikajanalla
    Tulos:     tapahtumat rajataan tavoitevuorokauteen ennen kirjoitusta

    Lookback/lookahead on 12 h, koska tyypit 8 ja 10 ovat jaksoja joilla on alku ja
    loppu -- vuorokauden rajan yli jatkuvat jaksot on saatava kokonaisina.

KÄSINAJO
    DAG-parametri "target_date" antaa UI:hin pelkän päivävalitsimen ilman
    kellonaikaa. Päivä on paikallinen kalenteripäivä. Tyhjänä käytetään ajon
    data_intervalin alkupään päivää. Päivävälit ajetaan
    backfillillä, joka ottaa jo valmiiksi pelkät päivämäärät.

KOHDETAULU
    DAG EI LUO TAULUA eikä skeemaa. Kanta omistaa skeemansa, ja kohdetaulun on
    oltava olemassa tietomallin mukaisena ennen ensimmäistä ajoa. Ennen jokaista
    kirjoitusta tarkistetaan, että taulu löytyy ja siinä on kaikki tarvittavat
    sarakkeet -- puuttuva sarake kaataa ajon nimeltä mainiten. Ylimääräiset
    sarakkeet eivät haittaa.

TUNNUKSET
    Venvissä ei ole Airflow'ta, joten Connectioneja ei voi lukea. Influx-osoite,
    token ja PostgreSQL-yhteys luetaan .env-tiedostosta venv-hakemistosta.
    Ks. .env.example.

TUNNUS JA UUDELLEENAJO
    machine_event_id on kohdekannassa bigserial, jonka kanta generoi -- DAG ei täytä
    sitä. Riveillä ei siis ole DAGin puolelta ennustettavaa tunnusta, joten
    replace_day kohdistaa poistonsa muihin kenttiin: workcenter_id, event_type_id ja
    partition rajaava päivä (tyypillä 10 stop_date, muilla start_date).

    Näin sama vuorokausi voidaan ajaa uudelleen: poisto osuu tasan tämän DAGin samalle
    päivälle kirjoittamiin riveihin eikä naapuripäiviin tai toisiin työkeskuksiin.

MUUT KIRJOITTAJAT SAMAAN TAULUUN
    replace_day poistaa kaikki päivän rivit, jotka täsmäävät poistoehtoon. Jos jokin
    toinen prosessi kirjoittaa samaan tauluun samalla source_id:llä, sen rivit
    poistuvat myös. Sellainen prosessi ei saa ajaa samaa vuorokautta tämän DAGin kanssa.

    Jos rinnakkaisia toteutuksia halutaan ajaa vertailua varten, tämän DAGin
    target.table on osoitettava eri tauluun asetustiedostossa.
"""

import os
from datetime import datetime, timedelta

from airflow.sdk import DAG, Param, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable

# Venv-hakemisto ja sen tulkki. Nämä ovat DAG-tason vakioita, mutta HUOM: niihin ei
# voi viitata taskien sisältä -- taskin runko ajetaan toisessa tulkissa, jossa tämän
# moduulin nimet eivät ole näkyvissä. Siksi polku toistuu jokaisen taskin rungossa.
VENV_DIR = "/opt/airflow/config/python-venvs/mtconnect_data_dag"
VENV_PYTHON = f"{VENV_DIR}/venv/bin/python"

# Aikakatkaisut. dagrun_timeout tulee env-muuttujasta koska se on DAG-konstruktorin
# argumentti eli evaluoituu parse-aikaan. Asettamattomana oletus pätee, joten
# docker-composeen ei tarvitse lisätä mitään.
DAGRUN_TIMEOUT = timedelta(minutes=int(os.getenv("MTCONNECT_DATA_DAGRUN_TIMEOUT_MIN", "25")))
FETCH_TIMEOUT = timedelta(minutes=5)
COMPUTE_TIMEOUT = timedelta(minutes=10)


# ======================================================================================
# TASKIT
#
# Jokainen taski on kolmerivinen kääre: sys.path, import, kutsu. Kaikki logiikka on
# mtconnect_data_core-paketissa, joka ajetaan venvin tulkilla. Taskit näkevät vain
# paketin julkisen rajapinnan (__init__.py), eivät sen sisäistä moduulijakoa.
#
# Miksi importit ovat funktion sisällä: @task.external_python sarjallistaa funktion
# RUNGON ja ajaa sen erillisessä prosessissa venvin tulkilla. Moduulitason nimet --
# importit, vakiot, muut funktiot -- eivät ole siellä käytettävissä, joten kaikki
# tarvittava on oltava rungon sisällä.
# ======================================================================================

# multiple_outputs=False on pakollinen jokaisessa dictin palauttavassa taskissa.
# Ilman sitä @task päättelee paluutyypin annotaatiosta multiple_outputs=True, jolloin
# Airflow työntää jokaisen avaimen omana XCominaan. Tuotannon Airflow 3.0.4 kaatuu
# siihen heti kun yksikin arvo on None: arvo sarjallistuu JSON-nullaksi, ja
# execution-API:n Body()-parametri tulkitsee nullin puuttuvaksi kentäksi
# (HTTP 422 "Field required"). Kehityksen 3.3.0 ei kaadu, joten vika ei näy täällä.
# Kukaan ei myöskään pulli yksittäisiä avaimia -- tulokset välitetään kokonaisina.
@task(multiple_outputs=False)
def resolve_run_context() -> dict:
    """Poimii ajon kontekstin sanakirjaksi venv-taskeja varten.

    Ainoa taski joka ajetaan Airflow'n tulkissa: get_current_context() ei toimi
    venvissä. Arvot sarjallistuvat XComiin, joten aikaleimat muunnetaan
    merkkijonoiksi -- core-moduuli jäsentää ne takaisin.

    Tavoitepäivä ratkaistaan TÄSSÄ, ei core-moduulissa:

        Ajopäivä-parametri   käsinajon valinta voittaa aina
        muuten               data_interval_startin kalenteripäivä

    Jälkimmäinen on se, mikä pitää laskettavan vuorokauden kalenterivuorokautena
    vaikka ajo laukeaa vasta klo 04:00 UTC. Aikataulun data_interval on
    [eilen 04:00, tänään 04:00], jonka alkupää osuu edelliseen vuorokauteen -- juuri
    siihen jonka data on nyt kokonaisuudessaan Influxissa. Ilman tätä core-moduulin
    _resolve_interval käyttäisi data_intervalia sellaisenaan ja laskisi vuorokauden
    rajat väärin. Ks. aikataulun perustelu DAG-määrittelystä.

    Päivä poimitaan tässä UTC-kalenterista, mutta core-moduuli tulkitsee sen
    PAIKALLISEKSI päiväksi. Ne ovat sama päivä, koska data_interval alkaa klo 04:00
    UTC eli Suomessa klo 06-07. Jos ajohetki siirretään UTC-iltaan (klo 21 jälkeen),
    päivät eroavat ja tämä on muutettava.
    """
    ctx = get_current_context()
    # Käsin käynnistetyllä ajolla ei välttämättä ole data_intervalia lainkaan (esim.
    # "airflow dags trigger" ilman loogista päivää). Silloin Ajopäivä-parametri riittää.
    interval_start = ctx.get("data_interval_start")
    interval_end = ctx.get("data_interval_end")
    target_date = (ctx.get("params") or {}).get("target_date")
    if not target_date and interval_start is None:
        raise ValueError(
            "Ajolla ei ole data_intervalia eikä Ajopäivä-parametria (target_date). "
            "Anna laskettava päivä parametrina, esim. {\"target_date\": \"2026-09-07\"}."
        )

    run_context = {
        "run_id": ctx["run_id"],
        "target_date": target_date or interval_start.date().isoformat(),
    }
    # Puuttuvia arvoja ei viedä XComiin tyhjinä -- core-moduuli ei tarvitse niitä kun
    # target_date on annettu.
    if interval_start is not None and interval_end is not None:
        run_context["data_interval_start"] = interval_start.isoformat()
        run_context["data_interval_end"] = interval_end.isoformat()
    return run_context


@task.external_python(python=VENV_PYTHON, expect_airflow=False, multiple_outputs=False)
def load_config() -> dict:
    """Lukee asetukset mtconnect_data_config.yaml:sta."""
    import sys
    sys.path.insert(0, "/opt/airflow/config/python-venvs/mtconnect_data_dag")
    import mtconnect_data_core as core
    return core.load_config()


@task.external_python(python=VENV_PYTHON, expect_airflow=False)
def plan_influx_queries_for_interval(config: dict, ctx: dict) -> list[dict]:
    """Muodostaa ajettavat kyselyt aikaikkunoineen."""
    import sys
    sys.path.insert(0, "/opt/airflow/config/python-venvs/mtconnect_data_dag")
    import mtconnect_data_core as core
    return core.plan_influx_queries_for_interval(config, ctx)


@task.external_python(python=VENV_PYTHON, expect_airflow=False, multiple_outputs=False,
                      retries=3, retry_delay=timedelta(minutes=2),
                      retry_exponential_backoff=True,
                      max_retry_delay=timedelta(minutes=10),
                      execution_timeout=FETCH_TIMEOUT)
def fetch_influx_query_to_staging(job: dict, config: dict, ctx: dict) -> dict:
    """Ajaa yhden Flux-kyselyn staging-hakemistoon.

    Ainoa verkkoa käyttävä taski, joten uudelleenyritykset ovat täällä: Influx tai
    VPN voi olla hetkellisesti tavoittamattomissa.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/config/python-venvs/mtconnect_data_dag")
    import mtconnect_data_core as core
    return core.fetch_influx_query_to_staging(job, config, ctx)


@task
def collect_fetch_results(fetched: list[dict]) -> list[dict]:
    """Muuntaa dynaamisen hakuhaaran tuloksen tavalliseksi listaksi.

    fetch_influx_query_to_staging on dynaamisesti monistettu (.expand), ja Airflow
    tarjoaa sen tuloksen laiskana LazyXComSelectSequence-oliona. Venv-taskille
    argumentit sarjallistetaan picklellä, eikä laiska sekvenssi kelpaa siihen: se ei
    ole lista eikä proxy, joten operaattorin resolve_proxies jättää sen ennalleen ja
    pickle yrittää sarjallistaa koko oliograafin -- sitä kautta DAG-objektin ja sen
    kaikkien taskien funktiot, mikä kaatuu PicklingErroriin.

    Tämä taski ajetaan Airflow'n tulkissa, jossa laiska sekvenssi toimii normaalisti.
    Se palauttaa saman sisällön tavallisena listana pieniä sanakirjoja.
    """
    return list(fetched)


@task.external_python(python=VENV_PYTHON, expect_airflow=False,
                      execution_timeout=COMPUTE_TIMEOUT)
def combine_and_validate_raw_data(config: dict, ctx: dict, fetched: list[dict]) -> str:
    """Yhdistää kyselyt, siirtää aikaleimat ja tarkistaa kentät."""
    import sys
    sys.path.insert(0, "/opt/airflow/config/python-venvs/mtconnect_data_dag")
    import mtconnect_data_core as core
    return core.combine_and_validate_raw_data(config, ctx, fetched)


@task.external_python(python=VENV_PYTHON, expect_airflow=False,
                      execution_timeout=COMPUTE_TIMEOUT)
def classify_machine_state_segments(config: dict, ctx: dict, combined_path: str) -> str:
    """Tilahaara: tilaluokittelu, segmentit, ohjelma ja override-lukemat."""
    import sys
    sys.path.insert(0, "/opt/airflow/config/python-venvs/mtconnect_data_dag")
    import mtconnect_data_core as core
    return core.classify_machine_state_segments(config, ctx, combined_path)


@task.external_python(python=VENV_PYTHON, expect_airflow=False,
                      execution_timeout=COMPUTE_TIMEOUT)
def compute_part_cycle_events(config: dict, ctx: dict, combined_path: str) -> str:
    """Kappalehaara: vaihtumishetket, sykliaikalaskurit ja override-lukemat."""
    import sys
    sys.path.insert(0, "/opt/airflow/config/python-venvs/mtconnect_data_dag")
    import mtconnect_data_core as core
    return core.compute_part_cycle_events(config, ctx, combined_path)


@task.external_python(python=VENV_PYTHON, expect_airflow=False,
                      execution_timeout=COMPUTE_TIMEOUT)
def build_raw_event_rows(config: dict, ctx: dict, state_path: str, part_path: str) -> str:
    """Raakataso: yhdistää haarat ja rajaa tapahtumat vuorokauteen."""
    import sys
    sys.path.insert(0, "/opt/airflow/config/python-venvs/mtconnect_data_dag")
    import mtconnect_data_core as core
    return core.build_raw_event_rows(config, ctx, state_path, part_path)


@task.external_python(python=VENV_PYTHON, expect_airflow=False, multiple_outputs=False,
                      execution_timeout=COMPUTE_TIMEOUT)
def write_daily_event_partition(config: dict, ctx: dict, raw_path: str) -> dict:
    """Kirjoittaa raakatason vuorokauden CSV-partitioksi (debug-menetelmä)."""
    import sys
    sys.path.insert(0, "/opt/airflow/config/python-venvs/mtconnect_data_dag")
    import mtconnect_data_core as core
    return core.write_daily_event_partition(config, ctx, raw_path)


@task.external_python(python=VENV_PYTHON, expect_airflow=False, multiple_outputs=False,
                      execution_timeout=COMPUTE_TIMEOUT)
def merge_partitions_to_combined_csv(config: dict, partition: dict) -> dict:
    """Kokoaa tuoreimmat partitiot yhdeksi koostetiedostoksi."""
    import sys
    sys.path.insert(0, "/opt/airflow/config/python-venvs/mtconnect_data_dag")
    import mtconnect_data_core as core
    return core.merge_partitions_to_combined_csv(config, partition)


@task.external_python(python=VENV_PYTHON, expect_airflow=False,
                      execution_timeout=COMPUTE_TIMEOUT)
def build_machine_events(config: dict, ctx: dict, raw_path: str) -> str:
    """Johtaa kuusi tapahtumatyyppiä raakatasosta."""
    import sys
    sys.path.insert(0, "/opt/airflow/config/python-venvs/mtconnect_data_dag")
    import mtconnect_data_core as core
    return core.build_machine_events(config, ctx, raw_path)


@task.external_python(python=VENV_PYTHON, expect_airflow=False, multiple_outputs=False,
                      execution_timeout=COMPUTE_TIMEOUT)
def load_to_postgres(config: dict, ctx: dict, events_path: str) -> dict:
    """Kirjoittaa konetapahtumat PostgreSQL-kohdetauluun.

    Ajon lopputulos. Lokiriviltä näkee poistettujen ja lisättyjen rivien määrät.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/config/python-venvs/mtconnect_data_dag")
    import mtconnect_data_core as core
    return core.load_to_postgres(config, ctx, events_path)


@task.external_python(python=VENV_PYTHON, expect_airflow=False,
                      trigger_rule="all_success")
def cleanup_run_staging_files(config: dict, ctx: dict, merged: dict, loaded: dict) -> None:
    """Poistaa onnistuneen ajon välidatan.

    trigger_rule="all_success": jos jokin edeltävä taski kaatui, tätä ei ajeta ja
    välitulokset jäävät levylle selvitystä varten.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/config/python-venvs/mtconnect_data_dag")
    import mtconnect_data_core as core
    core.cleanup_run_staging_files(config, ctx, merged, loaded)


# ======================================================================================
# DAGIN KOKOONPANO
#
# Riippuvuuksia ei kirjoiteta erikseen: kun taskin paluuarvo annetaan toisen taskin
# argumentiksi, Airflow päättelee järjestyksen siitä.
# ======================================================================================

with DAG(
    dag_id="mtconnect_data_dag",
    description="Konedatan tapahtumamalli: InfluxDB -> konetapahtumat -> PostgreSQL",
    # Ajo klo 04:00 UTC = 07:00 Suomen kesäaikaa (06:00 talvella). Kiinteä UTC-hetki
    # valittiin tarkoituksella: ajohetki ei hyppää kellonvaihdoksissa.
    #
    # CronTriggerTimetable EIKÄ CronDataIntervalTimetable, koska laukaisuhetki ja
    # laskettava vuorokausi on pidettävä erillään. Intervallipohjaisessa aikataulussa
    # cron määrää MOLEMMAT, jolloin klo 4 ajo laskisi ikkunan 04:00 -> 04:00 eikä
    # kalenterivuorokautta. Tässä data_interval on [eilen 04:00, tänään 04:00] UTC, ja
    # resolve_run_context johtaa siitä tavoitepäiväksi sen alkupään kalenteripäivän --
    # eli edellisen vuorokauden 00:00 -> 24:00. ("@daily" ei kelpaa: Airflow 3:ssa se on
    # trigger-pohjainen eikä tuota data_intervalia lainkaan.)
    schedule=CronTriggerTimetable("0 4 * * *", timezone="UTC", interval=timedelta(days=1)),
    start_date=datetime(2026, 8, 2),
    # Väliin jääneitä vuorokausia ei ajeta automaattisesti; ne ajetaan backfillillä.
    catchup=False,
    # Yksi ajo kerrallaan: rinnakkaiset ajot kirjoittaisivat samoja partitioita.
    max_active_runs=1,
    # Ilman tätä ajo voi jäädä roikkumaan vuorokausiksi jos stack on alhaalla.
    dagrun_timeout=DAGRUN_TIMEOUT,
    # Kun haku kaatuu lopullisesti, loppuja ei jäädä odottamaan.
    fail_fast=True,
    params={
        "target_date": Param(
            None,
            type=["string", "null"],
            format="date",
            title="Ajopäivä",
            description="Valitse päivä. Tyhjänä käytetään ajon omaa data_intervalia. "
                        "Päivävälit ajetaan backfillillä.",
        ),
    },
    tags=["mtconnect", "influx", "postgres", "venv"],
    doc_md=__doc__,
) as dag:
    # 1. Ajon konteksti ja asetukset. Molemmat menevät argumenttina joka taskille,
    #    joten koko ajo käyttää samoja arvoja.
    ctx = resolve_run_context()
    config = load_config()

    # 2. Haku. .expand luo yhden taskin per suunniteltu kysely, ja ne ajetaan
    #    rinnakkain. .partial antaa argumentit jotka ovat kaikille samat.
    fetched = fetch_influx_query_to_staging.partial(config=config, ctx=ctx).expand(
        job=plan_influx_queries_for_interval(config, ctx)
    )
    # collect_fetch_results materialisoi laiskan tuloksen listaksi -- ks. sen docstring.
    combined = combine_and_validate_raw_data(config, ctx, collect_fetch_results(fetched))

    # 3. Raakataso. Tila- ja kappalehaara laskevat rinnakkain samasta datasta.
    raw_events = build_raw_event_rows(
        config,
        ctx,
        classify_machine_state_segments(config, ctx, combined),
        compute_part_cycle_events(config, ctx, combined),
    )

    # 4. Kaksi tulosta samasta raakatasosta, toisistaan riippumatta:
    #    CSV-partitio ja kooste (debug) sekä konetapahtumat PostgreSQL:ään.
    merged = merge_partitions_to_combined_csv(
        config, write_daily_event_partition(config, ctx, raw_events))
    loaded = load_to_postgres(config, ctx, build_machine_events(config, ctx, raw_events))

    # 5. Siivous. Odottaa molempia haaroja, jotta välidata on varmasti tarpeetonta.
    cleanup_run_staging_files(config, ctx, merged, loaded)
