"""
raakataso.py -- raakatason laskenta ("data v1").

Signaaleista kaksi rivilajia (event_type-sarake):
    RAW_EVENT_STATE  yhtenäinen jakso jossa kone oli samassa tilassa (alku + kesto)
    RAW_EVENT_PART   hetki jolloin kappalelaskuri kasvoi (sykliaikoineen)

Syöte on yhdistetty data, jossa signaalit on jo käännetty loogisiksi nimiksi
(SIGNAL_COLUMN) ja laskurit sekunneiksi -- ks. influx._to_signals.

Tulos kirjoitetaan CSV-partitioiksi ja on samalla konetapahtumien syöte.
Puhdasta pandas- ja stdlib-koodia: ei .env-lukua eikä tietokantayhteyksiä.
"""

import csv
import os
from pathlib import Path

import pandas as pd

from .vakiot import (
    COUNTER_SIGNALS,
    CYCLE_DELTA_COLUMNS,
    EVENT_COLUMNS,
    OVERRIDE_SIGNALS,
    PIECE_CYCLE_COUNTERS,
    RAW_EVENT_PART,
    RAW_EVENT_STATE,
    SIGNAL_COLUMN,
    SIGNAL_FEED_OVERRIDE,
    SIGNAL_OPERATING_TIME,
    SIGNAL_PART_COUNT,
    SIGNAL_PROGRAM,
    SIGNAL_RUNNING_TIME,
    STATE_EFFICIENT,
    STATE_IDLE,
    STATE_INEFFICIENT,
    STATE_POWER_OFF,
    cycle_delta_column,
)


def _signal_rows(combined: pd.DataFrame, signal: str) -> pd.DataFrame:
    """Yhden signaalin lukemat: time ja value."""
    return combined[combined[SIGNAL_COLUMN] == signal][["time", "value"]]


def _classify_states(combined: pd.DataFrame, period_start: pd.Timestamp, period_end: pd.Timestamp,
                     aggr: str, threshold: float, override_threshold: float) -> pd.DataFrame:
    """Päättelee koneen tilan laskureista ja kokoaa peräkkäiset samat tilat jaksoiksi.

    Idea: laskurit ovat kumulatiivisia sekuntimittareita, joten koneen tila näkyy
    siinä, kuinka paljon ne kasvavat minuutissa.

        running_time kasvaa    -> kone on päällä
        operating_time kasvaa  -> ohjelma ajossa eli työstöä
        feed_override riittävä -> työstö on tehokasta, muuten tehotonta
        ei kasvua              -> kone pois päältä

    Vaiheet:
        1. laskurit taulukoksi ja minuuttikohtaisiksi kasvuiksi
        2. syöttöarvon override samaan aikajakoon
        3. tila jokaiselle aikaikkunalle kynnysten perusteella
        4. peräkkäiset samat tilat yhdeksi jaksoksi (alku, loppu, kesto)

    Palauttaa: start, end, state, duration_min -- yksi rivi per jakso.
    """
    # --- 1. Laskurit minuuttikohtaisiksi kasvuiksi ---
    counters = combined[combined[SIGNAL_COLUMN].isin(COUNTER_SIGNALS)].copy()
    counters["value"] = pd.to_numeric(counters["value"], errors="coerce")
    counters = counters.dropna(subset=["value"]).sort_values("time")

    # Yhtenäinen aika-akseli koko vuorokaudelle. Reunat pyöristetään ulospäin, jotta
    # ensimmäinen ja viimeinen ikkuna tulevat kokonaisina mukaan.
    full_range = pd.date_range(start=period_start.floor(aggr), end=period_end.ceil(aggr), freq=aggr)

    if counters.empty:
        # Ei laskuridataa lainkaan -> ei kasvua -> koko vuorokausi tulkitaan sammuksissa
        # olleeksi. Näin jatko toimii normaalisti eikä erikoistapauksia tarvita.
        delta_agg = pd.DataFrame(0.0, index=full_range, columns=COUNTER_SIGNALS)
    else:
        # Pitkä muoto leveäksi: yksi sarake per laskuri, rivit aikaleimoja.
        pivot = counters.pivot_table(index="time", columns=SIGNAL_COLUMN, values="value", aggfunc="last")
        # Laskuri säilyttää arvonsa mittausten välissä -> puuttuvat täytetään edellisellä.
        pivot = pivot.ffill().resample("1min").ffill()
        delta = pivot.diff()
        # Negatiivinen kasvu = laskuri nollattu (koneen uudelleenkäynnistys). Ei ole
        # todellinen tapahtuma, joten se pudotetaan nollaan.
        delta[delta < 0] = 0
        delta = delta.fillna(0)
        delta_agg = delta.resample(aggr).sum().reindex(full_range, fill_value=0)

    # --- 2. Syöttöarvon override samaan aikajakoon ---
    ovr = _signal_rows(combined, SIGNAL_FEED_OVERRIDE).copy()
    ovr["value"] = pd.to_numeric(ovr["value"], errors="coerce")
    ovr = ovr.dropna().sort_values("time")
    if ovr.empty:
        # Ei override-lukemia -> 0, jolloin työstö luokittuu tehottomaksi.
        feed_agg = pd.Series(0.0, index=full_range)
    else:
        # Override on asetusarvo joka pysyy voimassa muutokseen asti (ffill), ja
        # ikkunaa kohti siitä otetaan keskiarvo.
        feed_1min = ovr.set_index("time")["value"].resample("1min").ffill()
        feed_agg = feed_1min.resample(aggr).mean().reindex(full_range).fillna(0)

    # --- 3. Tila per aikaikkuna ---
    # Sijoitusjärjestys on merkitsevä: myöhempi ehto voittaa aiemman. Pois päältä on
    # lähtöoletus, jonka running_time kumoaa (Idle), jonka operating_time kumoaa (työstö).
    no_growth = pd.Series(0, index=delta_agg.index)
    state = pd.Series(STATE_POWER_OFF, index=delta_agg.index)
    state[delta_agg.get(SIGNAL_RUNNING_TIME, no_growth) > threshold] = STATE_IDLE
    tyosto_mask = delta_agg.get(SIGNAL_OPERATING_TIME, no_growth) > threshold
    tehokas_mask = tyosto_mask & (feed_agg >= override_threshold)
    state[tyosto_mask & ~tehokas_mask] = STATE_INEFFICIENT
    state[tehokas_mask] = STATE_EFFICIENT

    # --- 4. Peräkkäiset samat tilat yhdeksi jaksoksi ---
    state_df = pd.DataFrame({"start": state.index, "state": state.values})
    # Ikkunan loppu = seuraavan alku. Viimeiseltä riviltä loppu puuttuu -> pudotetaan.
    state_df["end"] = state_df["start"].shift(-1)
    state_df = state_df.dropna(subset=["end"]).copy()
    # Juokseva ryhmänumero: kasvaa aina kun tila vaihtuu edellisestä rivistä.
    state_df["grp"] = (state_df["state"] != state_df["state"].shift()).cumsum()

    segments = (
        state_df.groupby(["grp", "state"], as_index=False)
        .agg(start=("start", "first"), end=("end", "last"))
    )
    segments["duration_min"] = (segments["end"] - segments["start"]).dt.total_seconds() / 60
    return segments.drop(columns="grp").sort_values("start").reset_index(drop=True)


def _program_timeline(combined: pd.DataFrame) -> pd.DataFrame:
    """Ohjelmanimen muutoshetket aikajärjestyksessä.

    Arvo pysyy voimassa seuraavaan muutokseen asti, joten tästä haetaan _attach_program-
    funktiolla se ohjelma, joka oli käynnissä minäkin hetkenä.
    """
    return (
        _signal_rows(combined, SIGNAL_PROGRAM)
        .rename(columns={"value": "program"})
        .dropna().sort_values("time").reset_index(drop=True)
    )


def _attach_program(df: pd.DataFrame, time_col: str, pprogram: pd.DataFrame) -> pd.DataFrame:
    """Liittää riviin sillä hetkellä käynnissä olleen ohjelman (viimeisin tunnettu arvo).

    merge_asof direction="backward" etsii jokaiselle riville lähimmän AIEMMAN
    ohjelmamuutoksen -- ei siis lähintä ylipäätään, koska tulevaisuuden ohjelma ei ollut
    vielä käynnissä. Ennen ensimmäistä tunnettua muutosta arvoksi jää None.
    """
    df = df.sort_values(time_col).reset_index(drop=True)
    if pprogram.empty:
        df["program"] = None
        return df

    prog = pprogram.rename(columns={"time": "_prog_time"})
    df = pd.merge_asof(df, prog, left_on=time_col, right_on="_prog_time", direction="backward")
    return df.drop(columns="_prog_time")


def _compute_part_changes(combined: pd.DataFrame, pprogram: pd.DataFrame,
                          min_cycle_min: float, max_cycle_min: float) -> pd.DataFrame:
    """Kappalelaskurin vaihtumishetket ohjelmineen ja sykliaikoineen.

    Kappale on valmistunut silloin kun laskurin arvo kasvaa. Laskuria seurataan
    OHJELMA-AJOITTAIN: ajo alkaa kun ohjelman nimi vaihtuu, ja sen sisällä laskurin
    nollaus (lukema pienenee) aloittaa uuden kierroksen.

    Sykliaika (menetelmä C): kappaleen alku on uusin näistä -- edellinen kappale samalla
    kierroksella, laskurin nollaus tai ohjelman lataus. Näin myös ajon ensimmäinen
    kappale saa sykliajan, eikä nollausta edeltävä kappale venytä seuraavan sykliä.
    Ensimmäisen kappaleen aikaan sisältyy latauksen jälkeinen asetusaika.

    HUOM: kone toistaa ohjelmalukeman ja kappalelaskurin nollan samalla aikaleimalla myös
    ilman latausta, jopa kesken yhtäjaksoisen työstön (1.-22.9.2026: 43/151 toistoa).
    Siksi saman nimen toisto ei aloita ajoa eikä toistunut nolla ole nollaus -- vain
    nimen vaihtuminen ja laskurin todellinen putoaminen ratkaisevat.

    Sykliaika joka jää välin [min_cycle_min, max_cycle_min] ulkopuolelle tyhjennetään
    (calculation-osio).

    Palauttaa: time, value (laskurin lukema), program, delta (montako kappaletta),
    cycle_start (syklin alku), cycle_min (aika syklin alusta).
    """
    ppartcount = _signal_rows(combined, SIGNAL_PART_COUNT).copy()
    ppartcount["value"] = pd.to_numeric(ppartcount["value"], errors="coerce")
    ppartcount = ppartcount.dropna().sort_values("time").reset_index(drop=True)

    if ppartcount.empty or pprogram.empty:
        return pd.DataFrame(columns=["time", "value", "program", "delta", "cycle_start", "cycle_min"])

    # Ajo alkaa ohjelman nimen vaihtuessa; saman nimen toistot eivät aloita uutta.
    # Samalla aikaleimalla tuleva nollalukema kuuluu uuteen ajoon (merge_asof backward
    # ottaa yhtä suuren aikaleiman mukaan).
    runs = pprogram[pprogram["program"].ne(pprogram["program"].shift())]
    runs = runs.assign(_run=range(len(runs)), _load_at=runs["time"])

    # Ilman ohjelmatietoa lukemaa ei voi kohdistaa mihinkään -> rivi pudotetaan.
    part_prog = _attach_program(ppartcount, "time", runs)
    part_prog = part_prog.dropna(subset=["program"]).sort_values("time")

    part_prog["delta"] = part_prog.groupby("_run")["value"].diff()
    # Negatiivinen = laskuri nollattu, ei kappaleiden katoaminen. Ajon ensimmäiseltä
    # lukemalta erotus puuttuu (fillna) -- sitä ei voi verrata mihinkään.
    reset = part_prog["delta"] < 0
    part_prog.loc[reset, "delta"] = 0
    part_prog["delta"] = part_prog["delta"].fillna(0)

    # Kierros = ajon osa kahden nollauksen välissä. Kierroksen alku on nollaushetki,
    # ajon ensimmäisellä kierroksella ohjelman latausaika.
    part_prog["_round"] = reset.astype(int).groupby(part_prog["_run"]).cumsum()
    part_prog["_round_start"] = part_prog["time"].where(reset).fillna(part_prog["_load_at"])
    part_prog["_round_start"] = part_prog.groupby(["_run", "_round"])["_round_start"].transform("first")

    # Vain kasvaneet lukemat ovat tapahtumia; muut ovat saman arvon toistoa.
    part_changes = part_prog[part_prog["delta"] > 0].copy()
    part_changes["cycle_start"] = (
        part_changes.groupby(["_run", "_round"])["time"].shift()
        .fillna(part_changes["_round_start"]))
    part_changes["cycle_min"] = (
        part_changes["time"] - part_changes["cycle_start"]).dt.total_seconds() / 60
    part_changes = part_changes.drop(columns=["_run", "_load_at", "_round", "_round_start"])
    # Uskottavuusrajat sykliajalle: liian lyhyt on mittausartefakti ja liian pitkä
    # käytännössä taukoa kappaleiden välissä, ei todellista sykliaikaa. Rajojen
    # ulkopuolinen arvo tyhjennetään -- parempi puuttua kuin valehdella.
    plausible = part_changes["cycle_min"].between(min_cycle_min, max_cycle_min)
    part_changes.loc[~plausible, "cycle_min"] = pd.NA
    return part_changes.reset_index(drop=True)


def _attach_cycle_counters(part_changes: pd.DataFrame, combined: pd.DataFrame) -> pd.DataFrame:
    """Liittää kappaleeseen todella koneistukseen kuluneen ajan (laskurien erotus syklin yli).

    cycle_min kertoo kellonajan kappaleiden välillä, mutta osa siitä on odottelua. Tässä
    katsotaan paljonko laskurit kasvoivat saman ajan sisällä: se on koneen todellinen
    työskentelyaika. Erotus näiden välillä on vaihtotyötä (ks. _part_change_events).

    Lisää sarakkeet cutting_delta_min, operating_delta_min ja spindle_delta_min.
    """
    part_changes = part_changes.sort_values("time").reset_index(drop=True)

    if part_changes.empty:
        for col in CYCLE_DELTA_COLUMNS:
            part_changes[col] = pd.Series(dtype="float64")
        return part_changes

    for field in PIECE_CYCLE_COUNTERS:
        col = cycle_delta_column(field)
        counter = _signal_rows(combined, field).copy()
        counter["value"] = pd.to_numeric(counter["value"], errors="coerce")
        counter = counter.dropna().sort_values("time").rename(columns={"value": field})

        if counter.empty:
            part_changes[col] = None
            continue

        # Laskurin lukema syklin alussa ja kappaletapahtuman hetkellä -- sama väli kuin
        # cycle_minissä. Laskurit ovat sekunteja -> /60 minuuteiksi.
        at_end = pd.merge_asof(part_changes[["time"]], counter, on="time",
                               direction="backward")[field]
        at_start = pd.merge_asof(
            part_changes[["cycle_start"]].reset_index().sort_values("cycle_start"),
            counter.rename(columns={"time": "cycle_start"}),
            on="cycle_start", direction="backward",
        ).set_index("index").sort_index()[field]
        delta_min = (at_end - at_start) / 60
        # Negatiivinen = laskuri nollattu välissä; arvoa ei voi käyttää.
        delta_min[delta_min < 0] = pd.NA
        part_changes[col] = delta_min

    return part_changes


def _attach_overrides(df: pd.DataFrame, combined: pd.DataFrame, time_col: str = "time") -> pd.DataFrame:
    """Liittää riviin lähimmän tunnetun override-lukeman (raaka arvo, ei erotusta)."""
    df = df.sort_values(time_col).reset_index(drop=True)

    if df.empty:
        for field in OVERRIDE_SIGNALS:
            df[field] = pd.Series(dtype="float64")
        return df

    for field in OVERRIDE_SIGNALS:
        reading = _signal_rows(combined, field).copy()
        reading["value"] = pd.to_numeric(reading["value"], errors="coerce")
        reading = reading.dropna().sort_values("time").rename(
            columns={"value": field, "time": f"_{field}_time"})

        if reading.empty:
            df[field] = None
            continue

        df = pd.merge_asof(df, reading, left_on=time_col, right_on=f"_{field}_time", direction="backward")
        df = df.drop(columns=f"_{field}_time")

    return df


def _build_events(state_segments: pd.DataFrame, part_changes: pd.DataFrame) -> pd.DataFrame:
    """Yhdistää tilasegmentit ja kappaletapahtumat yhdeksi aikajärjestetyksi listaksi.

    Kaksi eri lajia mahtuu samaan taulukkoon, koska kummallakin on samat sarakkeet:
    event_type erottaa ne toisistaan ja toiselle lajille kuulumattomat kentät jäävät
    tyhjiksi. Tilarivillä on kesto, kappalerivillä laskurilukemat.
    """
    events = []

    # Tilajaksot: alku, tila ja kesto. Kappalekentät jäävät tyhjiksi.
    for _, row in state_segments.iterrows():
        event = {
            "time": row["start"],
            "event_type": RAW_EVENT_STATE,
            "program": row.get("program"),
            "state": row["state"],
            "duration_min": round(row["duration_min"], 2),
            "part_count": None,
            "part_delta": None,
            "cycle_min": None,
        }
        for col in CYCLE_DELTA_COLUMNS:
            event[col] = None
        for field in OVERRIDE_SIGNALS:
            value = row.get(field)
            event[field] = None if value is None or pd.isna(value) else round(value, 2)
        events.append(event)

    # Kappaletapahtumat: hetkellisiä, joten kestoa tai tilaa ei ole.
    for _, row in part_changes.iterrows():
        cycle_min = row.get("cycle_min")
        event = {
            "time": row["time"],
            "event_type": RAW_EVENT_PART,
            "program": row.get("program"),
            "state": None,
            "duration_min": None,
            "part_count": row["value"],
            "part_delta": row["delta"],
            "cycle_min": None if pd.isna(cycle_min) else round(cycle_min, 2),
        }
        for col in CYCLE_DELTA_COLUMNS:
            value = row.get(col)
            event[col] = None if value is None or pd.isna(value) else round(value, 2)
        for field in OVERRIDE_SIGNALS:
            value = row.get(field)
            event[field] = None if value is None or pd.isna(value) else round(value, 2)
        events.append(event)

    if not events:
        return pd.DataFrame(columns=EVENT_COLUMNS)

    events_df = pd.DataFrame(events).sort_values("time").reset_index(drop=True)
    # astype(object) jotta tyhjät kentät kirjoittuvat CSV:hen tyhjinä ("nan"-tekstin sijaan)
    return events_df.astype(object).where(pd.notna(events_df), None)


def _write_csv(rows: list[dict], path: Path) -> None:
    """Kirjoittaa CSV:n atomisesti. Tyhjäkin tulos kirjoittaa otsikkorivin."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=EVENT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)
