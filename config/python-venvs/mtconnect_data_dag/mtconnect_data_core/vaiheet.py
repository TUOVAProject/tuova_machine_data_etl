"""
vaiheet.py -- DAGin taskien kutsumat funktiot.

Nämä funktiot vastaavat yksi yhteen DAGin taskeja: mtconnect_data_dag.py:n jokainen
@task.external_python -kääre kutsuu tasan yhtä näistä. Kukin hakee syötteen levyltä,
kutsuu muiden moduulien laskentaa, kirjoittaa tuloksen levylle ja lokittaa mitä
tapahtui.

Paluuarvot ovat polkuja ja pieniä sanakirjoja, koska ne kulkevat taskien välillä
XComissa. DataFramet kulkevat LEVYN kautta (staging/<run_id>/*.pkl) -- XCom tallentuu
Airflow'n metadatakantaan, johon konedatan kokoiset taulukot eivät kuulu.

ctx on DAG-puolelta tuleva sanakirja, ei Airflow-konteksti.
"""

import os
import shutil
import time

import pandas as pd

from .apurit import (
    _flux_time,
    _from_influx,
    _log,
    _output_dir,
    _partition_ds,
    _resolve_interval,
    _staging_dir,
    _staging_root,
    _target_period,
    _to_influx,
    _to_local,
)
from .asetukset import _load_config
from .influx import (
    _bucket,
    _job_query,
    _missing_signals,
    _query_influx,
    _signal_queries,
    _to_signals,
)
from .konetapahtumat import _build_machine_events
from .postgres import _write_machine_events
from .raakataso import (
    _attach_cycle_counters,
    _attach_overrides,
    _attach_program,
    _build_events,
    _classify_states,
    _compute_part_changes,
    _program_timeline,
    _write_csv,
)
from .vakiot import (
    EVENT_COLUMNS,
    JOB_MAIN,
    JOB_WARMUP,
    QUERY_WARMUP,
    SIGNAL_COLUMN,
    WARMUP_AGG_FN,
)


def load_config() -> dict:
    """Lukee ja tarkistaa asetukset mtconnect_data_config.yaml:sta.

    Tiedosto on ainoa totuus: sen puuttuminen tai vanha muoto kaataa ajon, puuttuva
    yksittäinen avain täydentyy oletuksesta. Ks. asetukset.py.
    """
    return _load_config()


def plan_influx_queries_for_interval(config: dict, ctx: dict) -> list[dict]:
    """Muodostaa ajettavat kyselyt aikaikkunoineen: varsinaiset haut + lämmittelyhaut.

    Ei hae mitään -- palauttaa listan tehtäviä, joista Airflow luo yhden
    fetch_influx_query_to_staging-taskin per alkio (.expand). Kyselyt ajetaan siis
    rinnakkain ja yksi hidas haku ei jarruta muita. Haut johdetaan signals-osiosta
    (influx._signal_queries).

    Lämmittelyhaut noutavat kentän viimeisimmän arvon ENNEN hakuikkunaa: esimerkiksi
    ohjelman nimi ei välttämättä muutu kertaakaan vuorokauden aikana, jolloin sitä ei
    löytyisi ikkunan sisältä lainkaan.
    """
    # ctx tulee argumenttina -- Airflow-kontekstia ei ole venvissä.
    start, end = _resolve_interval(config, ctx)

    lookback = pd.Timedelta(hours=float(config["time"]["lookback_hours"]))
    lookahead = pd.Timedelta(hours=float(config["time"]["lookahead_hours"]))
    warmup_days = int(config["time"]["warmup_days"])

    # Aikaleimojen korjaus, kohta 1/2.
    # Influxin aikaleimat ovat kellovirheen verran jäljessä. Haku on siksi tehtävä
    # Influxin omalla, virheellisellä aikajanalla: tavoiteikkuna miinus kellovirhe.
    # Ilman tätä partitio ei kattaisi paikallista vuorokautta 00:00 -> 24:00.
    fetch_start = _to_influx(start, config) - lookback
    fetch_stop = _to_influx(end, config) + lookahead
    jobs = []
    for query in _signal_queries(config):
        job = {"key": query["key"], "measurement": query["measurement"], "fields": query["fields"]}
        if query["mode"] == QUERY_WARMUP:
            # aggregateWindow fn: last -> kentän viimeisin arvo ennen hakuikkunaa
            job.update(kind=JOB_WARMUP, window=f"{warmup_days}d", agg_fn=WARMUP_AGG_FN,
                       start=_flux_time(fetch_start - pd.Timedelta(days=warmup_days)),
                       stop=_flux_time(fetch_start))
        else:
            job.update(kind=JOB_MAIN, window=query.get("window"), agg_fn=query.get("agg_fn"),
                       start=_flux_time(fetch_start), stop=_flux_time(fetch_stop))
        jobs.append(job)

    _log("suunniteltu", ds=_partition_ds(config, ctx), ikkuna=[str(start), str(end)],
         ikkuna_paikallinen=[str(_to_local(start, config)), str(_to_local(end, config))],
         lookback_h=config["time"]["lookback_hours"], lookahead_h=config["time"]["lookahead_hours"],
         kyselyt=[job["key"] for job in jobs])
    return jobs


def fetch_influx_query_to_staging(job: dict, config: dict, ctx: dict) -> dict:
    """Ajaa yhden Flux-kyselyn ja tallentaa tuloksen ajokohtaiseen staging-hakemistoon.

    Ainoa verkkoa käyttävä taski, joten uudelleenyritykset ovat täällä: Influx tai VPN
    voi olla hetkellisesti tavoittamattomissa. Palauttaa polun ja rivimäärän, ei dataa.
    """
    # ctx tulee argumenttina -- Airflow-kontekstia ei ole venvissä.
    frame = _query_influx(config, _job_query(config, job, _bucket(config)))

    path = _staging_dir(config, ctx) / f"raw_{job['key']}.pkl"
    frame.to_pickle(path)
    _log("haettu", kysely=job["key"], laji=job["kind"], riveja=len(frame))
    return {"key": job["key"], "kind": job["kind"], "path": str(path), "rows": len(frame)}


def combine_and_validate_raw_data(config: dict, ctx: dict, fetched: list[dict]) -> str:
    """Yhdistää kyselyt, kääntää signaalit, siirtää aikaleimat ja tarkistaa datan.

    Portti laskennan eteen: tästä eteenpäin oletetaan että data on olemassa, oikeassa
    ajassa ja loogisilla signaalinimillä (SIGNAL_COLUMN, laskurit sekunteina). Tyhjä tai
    vajaa haku kaataa ajon tässä, koska myöhempi laskenta tuottaisi siitä oikean
    näköisiä mutta vääriä lukuja.
    """
    # ctx tulee argumenttina -- Airflow-kontekstia ei ole venvissä.

    # Lämmittelykysely saa olla tyhjä (kenttä voi olla uusi), varsinainen ei.
    empty = sorted(item["key"] for item in fetched if item["rows"] == 0 and item["kind"] == JOB_MAIN)
    if empty:
        raise ValueError(
            f"Kyselyt {', '.join(empty)} palauttivat 0 riviä -- tulos olisi rakenteellisesti väärä."
        )

    frames = [pd.read_pickle(item["path"]) for item in sorted(fetched, key=lambda item: item["key"])]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        raise ValueError("Yksikään hakulauseke ei palauttanut dataa.")

    combined = _to_signals(config, pd.concat(frames, ignore_index=True))
    combined["time"] = pd.to_datetime(combined["time"], utc=True)
    # Vakaa lajittelu: saman aikaleiman rivit pysyvät hakujen järjestyksessä.
    combined = combined.sort_values("time", kind="mergesort").reset_index(drop=True)
    # Aikaleimojen korjaus, kohta 2/2: Influxin aikaleimat siirretään oikeaan
    # UTC-aikaan, jossa koko laskenta tehdään. Paikalliseen aikaan muunnetaan vasta
    # ulos kirjoitettaessa. Ks. AIKALEIMOJEN KORJAUS DAG-tiedoston docstringissä.
    combined["time"] = _from_influx(combined["time"], config)

    missing = _missing_signals(config, combined)
    if missing:
        raise ValueError(
            f"Haetusta datasta puuttuvat signaalit kokonaan: {', '.join(missing)}. "
            "Tarkista signals-osion measurement ja name.")

    path = _staging_dir(config, ctx) / "combined.pkl"
    combined.to_pickle(path)
    _log("yhdistetty", riveja=len(combined), signaalit=sorted(combined[SIGNAL_COLUMN].unique()))
    return str(path)


def classify_machine_state_segments(config: dict, ctx: dict, combined_path: str) -> str:
    """Tilahaara: tilaluokittelu, segmentit, ohjelma ja override-lukemat.

    Rinnakkainen compute_part_cycle_events-taskin kanssa; molemmat lukevat saman
    combined.pkl:n ja build_raw_event_rows yhdistää tulokset.
    """
    # ctx tulee argumenttina -- Airflow-kontekstia ei ole venvissä.
    combined = pd.read_pickle(combined_path)
    period_start, period_end = _target_period(config, ctx)

    segments = _classify_states(
        combined, period_start, period_end,
        aggr=config["calculation"]["aggr"],
        threshold=float(config["calculation"]["threshold"]),
        override_threshold=float(config["calculation"]["override_threshold"]),
    )
    segments = _attach_program(segments, "start", _program_timeline(combined))
    segments = _attach_overrides(segments, combined, time_col="start")

    path = _staging_dir(config, ctx) / "state_segments.pkl"
    segments.to_pickle(path)
    _log("tilasegmentit", maara=len(segments))
    return str(path)


def compute_part_cycle_events(config: dict, ctx: dict, combined_path: str) -> str:
    """Kappalehaara: vaihtumishetket, sykliaikalaskurit ja override-lukemat.

    Rinnakkainen classify_machine_state_segments-taskin kanssa.
    """
    # ctx tulee argumenttina -- Airflow-kontekstia ei ole venvissä.
    combined = pd.read_pickle(combined_path)

    part_changes = _compute_part_changes(
        combined, _program_timeline(combined),
        min_cycle_min=float(config["calculation"]["min_cycle_min"]),
        max_cycle_min=float(config["calculation"]["max_cycle_min"]),
    )
    part_changes = _attach_cycle_counters(part_changes, combined)
    part_changes = _attach_overrides(part_changes, combined)

    path = _staging_dir(config, ctx) / "part_changes.pkl"
    part_changes.to_pickle(path)
    _log("kappaletapahtumat", maara=len(part_changes))
    return str(path)


def build_raw_event_rows(config: dict, ctx: dict, state_path: str, part_path: str) -> str:
    """Raakataso ("data v1"): yhdistää haarat ja rajaa tapahtumat vuorokauteen.

    Haku ulottui lookback/lookahead-tuntien verran vuorokauden ulkopuolelle, jotta
    rajan yli jatkuvat jaksot saatiin kokonaisina. Ylimääräiset rivit karsitaan tässä.

    Tämä tulos on sekä CSV-partition että konetapahtumien lähde.
    """
    # ctx tulee argumenttina -- Airflow-kontekstia ei ole venvissä.
    events = _build_events(pd.read_pickle(state_path), pd.read_pickle(part_path))

    # Mukaan alkuhetki, ulos loppuhetki -- muuten vuorokauden viimeinen tapahtuma
    # päätyisi kahteen partitioon.
    period_start, period_end = _target_period(config, ctx)
    if not events.empty:
        times = pd.to_datetime(events["time"], utc=True)
        events = events[(times >= period_start) & (times < period_end)].reset_index(drop=True)

    path = _staging_dir(config, ctx) / "raw_events.pkl"
    events.to_pickle(path)
    _log("raakataso", ds=_partition_ds(config, ctx), riveja=len(events),
         rajaus=[str(period_start), str(period_end)])
    return str(path)


def write_daily_event_partition(config: dict, ctx: dict, raw_path: str) -> dict:
    """Kirjoittaa raakatason vuorokauden partitioksi.

    Debug-menetelmä, ei kohdejärjestelmä: CSV:stä näkee mistä riveistä konetapahtumat
    syntyivät, kun luku PostgreSQL:ssä näyttää oudolta. Sama päivä uudelleen ajettuna
    korvaa oman tiedostonsa.

    Aikaleimat kirjoitetaan paikallisena aikana UTC-erotuksen kanssa (esim.
    2026-10-06 08:15:00+03:00): rivi on luettavissa seinäkelloaikana, ja kellonsiirron
    toistuva tunti pysyy silti yksiselitteisenä.
    """
    # ctx tulee argumenttina -- Airflow-kontekstia ei ole venvissä.
    events = pd.read_pickle(raw_path)
    events["time"] = [str(_to_local(stamp, config)) for stamp in events["time"]]

    ds = _partition_ds(config, ctx)
    output_file = _output_dir(config) / f"{config['files']['partition_prefix']}_{ds}.csv"
    _write_csv(events.to_dict(orient="records"), output_file)

    _log("partitio_kirjoitettu", ds=ds, riveja=len(events), tiedosto=str(output_file))
    return {"path": str(output_file), "rows": len(events)}


def merge_partitions_to_combined_csv(config: dict, partition: dict) -> dict:
    """Kokoaa tuoreimmat partitiot yhdeksi koostetiedostoksi.

    Koko koostetiedosto kirjoitetaan joka ajolla uudelleen partitioista, jolloin
    korjattu partitio korjaa myös koosteen. Partitiot luetaan merkkijonoina, jottei
    numeroiden muotoilu muutu matkalla.
    """
    output_dir = _output_dir(config)
    # Tiedostonimessä on päivä muodossa YYYY-MM-DD, joten aakkosjärjestys on samalla
    # aikajärjestys ja lopusta saa tuoreimmat.
    files = sorted(output_dir.glob(f"{config['files']['partition_prefix']}_*.csv"))[-int(config["files"]["merge_window_days"]):]
    frames = [pd.read_csv(path, dtype=str, keep_default_na=False) for path in files]

    if frames:
        merged = pd.concat(frames, ignore_index=True)
        # Partitiot voivat mennä reunoiltaan päällekkäin; tuorein versio voittaa.
        merged = merged.drop_duplicates(subset=["time", "event_type", "program"], keep="last")
        merged = merged.sort_values("time", kind="mergesort").reset_index(drop=True)
    else:
        merged = pd.DataFrame(columns=EVENT_COLUMNS)

    # Kirjoitus tilapäistiedoston kautta: lukija ei koskaan näe puolivalmista koostetta.
    target = output_dir / config["files"]["combined_file"]
    tmp = target.with_name(target.name + ".tmp")
    merged.to_csv(tmp, index=False)
    os.replace(tmp, target)

    _log("kooste", riveja=len(merged), partitioita=len(files), uusin=partition["path"])
    return {"path": str(target), "rows": len(merged), "partitions": len(files)}


def build_machine_events(config: dict, ctx: dict, raw_path: str) -> str:
    """Johtaa kuusi tapahtumatyyppiä raakatasosta (osa 5).

    Lokiin kirjataan tapahtumien määrä tyypeittäin. Se on ensimmäinen paikka, josta
    poikkeava ajo huomataan: esimerkiksi nolla kappaletapahtumaa työpäivältä.
    """
    # ctx tulee argumenttina -- Airflow-kontekstia ei ole venvissä.
    period_start, period_end = _target_period(config, ctx)
    ds = _partition_ds(config, ctx)

    events = _build_machine_events(pd.read_pickle(raw_path), period_start, period_end, config)

    path = _staging_dir(config, ctx) / "machine_events.pkl"
    events.to_pickle(path)
    _log("konetapahtumat", ds=ds, yhteensa=len(events),
         per_tyyppi=events["event_type_id"].value_counts().sort_index().to_dict())
    return str(path)


def load_to_postgres(config: dict, ctx: dict, events_path: str) -> dict:
    """Kirjoittaa konetapahtumat ulkoiseen PostgreSQL-kantaan (osa 6).

    Ajon lopputulos. Lokiriviltä näkee poistettujen ja lisättyjen rivien määrät -- jos
    poistettuja on satoja ja lisättyjä muutama, päivä on jäänyt vajaaksi vaikka taski
    menisi vihreänä läpi.
    """
    # ctx tulee argumenttina -- Airflow-kontekstia ei ole venvissä.
    events = pd.read_pickle(events_path)
    ds = _partition_ds(config, ctx)

    result = _write_machine_events(config, events, ds)
    _log("postgres", ds=ds, taulu=f"{config['target']['schema']}.{config['target']['table']}",
         per_tyyppi=events["event_type_id"].value_counts().sort_index().to_dict(), **result)
    return result


def cleanup_run_staging_files(config: dict, ctx: dict, merged: dict, loaded: dict) -> None:
    """Poistaa onnistuneen ajon välidatan. Epäonnistuneen ajon staging jää tutkittavaksi.

    trigger_rule="all_success" on tässä tarkoituksellinen: jos jokin edeltävä taski
    kaatui, tätä ei ajeta lainkaan ja välitulokset jäävät levylle selvitystä varten.
    Vanhat jäänteet siivotaan iän perusteella myöhemmillä ajoilla.
    """
    # ctx tulee argumenttina -- Airflow-kontekstia ei ole venvissä.
    shutil.rmtree(_staging_dir(config, ctx, create=False), ignore_errors=True)

    # Aiempien epäonnistuneiden ajojen jäänteet: pois kun ne ovat tarpeeksi vanhoja.
    cutoff = time.time() - int(config["files"]["staging_max_age_days"]) * 86400
    for path in _staging_root(config).glob("*"):
        if path.is_dir() and path.stat().st_mtime < cutoff:
            print(f"Poistetaan vanha staging: {path}")
            shutil.rmtree(path, ignore_errors=True)
