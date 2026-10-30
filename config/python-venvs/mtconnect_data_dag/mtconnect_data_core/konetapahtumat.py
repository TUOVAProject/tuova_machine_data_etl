"""
konetapahtumat.py -- määrittelyn tapahtumatyypit 8, 10, 11, 14, 16 ja 17.

Raakatason riveistä johdetaan kuusi tapahtumatyyppiä, jokainen omalla funktiollaan:
    _production_events    tyyppi 8   ohjelmakohtainen tuotantojakso
    _part_change_events   tyyppi 10  yksi kappale
    _state_events         tyypit 11, 14 ja 16  yksi tilajakso
    _day_event            tyyppi 17  vuorokauden koonti

Kaikki käyttävät samaa runkoa (_base_event) ja samoja laskuapureita, ja kaikki
kirjoitetaan samaan tauluun -- käyttämättömät kentät jäävät NULLiksi.

Tyypit ovat eri raekokoja samasta datasta, EIKÄ niitä ole tarkoitus summata keskenään:
sama minuutti esiintyy tarkoituksella sekä tilatapahtumassa, tuotantojaksossa että
vuorokauden koonnissa (määrittelyn kohta 10).

Sisääntulopiste on _build_machine_events.
"""

import pandas as pd

from .apurit import _local_day, _log, _to_local, _workcenter_id
from .vakiot import (
    CYCLE_DELTA_COLUMNS,
    EVENT_COLUMNS,
    EVENT_TYPE_CUTTING,
    EVENT_TYPE_DAY,
    EVENT_TYPE_IDLE,
    EVENT_TYPE_PART_CHANGE,
    EVENT_TYPE_POWER_OFF,
    EVENT_TYPE_PRODUCTION,
    MACHINE_EVENT_COLUMNS,
    OVERRIDE_SIGNALS,
    RAW_EVENT_PART,
    RAW_EVENT_STATE,
    SIGNAL_CUTTING_TIME,
    SIGNAL_FEED_OVERRIDE,
    SIGNAL_OPERATING_TIME,
    SIGNAL_SPINDLE_OVERRIDE,
    STATE_EFFICIENT,
    STATE_IDLE,
    STATE_INEFFICIENT,
    STATE_POWER_OFF,
    STATES,
    UNAVAILABLE_PROGRAM,
    cycle_delta_column,
)

# Raakatason sarakkeet, joihin laskurien kasvu yhden kappaleen aikana on kirjattu.
OPERATING_DELTA = cycle_delta_column(SIGNAL_OPERATING_TIME)
CUTTING_DELTA = cycle_delta_column(SIGNAL_CUTTING_TIME)


# --- Laskuapurit ---------------------------------------------------------------------
# Yhteinen sääntö: puuttuva tieto on None, ei nolla. Nolla väittäisi että mitattiin ja
# tulos oli nolla; None kertoo ettei tietoa ole.

def _num(value) -> float | None:
    """Numeroksi tai None. Raakatasolla tyhjät kentät ovat None-arvoja."""
    if value is None:
        return None
    result = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(result) else float(result)


def _total(values) -> float:
    """Summa joka ohittaa puuttuvat arvot. Tyhjästä joukosta 0.0."""
    return float(sum(value for value in values if value is not None))


def _average(values) -> float | None:
    """Keskiarvo joka ohittaa puuttuvat arvot. Tyhjästä joukosta None, ei nolla."""
    valid = [value for value in values if value is not None]
    return round(sum(valid) / len(valid), 2) if valid else None


def _rounded(value, digits: int = 2) -> float | None:
    """Pyöristys joka säilyttää None-arvon. Minuutit kirjataan kahdella desimaalilla."""
    return None if value is None else round(float(value), digits)


def _share(part, whole) -> float | None:
    """Osuus desimaalilukuna 0-1 (määrittelyn kohta 10). Nollajako -> None."""
    if part is None or not whole:
        return None
    return round(float(part) / float(whole), 4)


def _positive_gap(minuend, subtrahend) -> float:
    """MAX(0, a - b). Puuttuva arvo tulkitaan nollaksi.

    Käytetään kun erotuksen pitää olla aikaa: esimerkiksi "sykliaika miinus koneistusaika"
    on vaihtotyötä. Mittausepätarkkuus voi tehdä erotuksesta niukasti negatiivisen, mikä
    ei tarkoita mitään -- siksi nolla alarajana.
    """
    return max(0.0, (minuend or 0.0) - (subtrahend or 0.0))


def _date_part(value) -> str | None:
    """Aikaleiman päiväosa muodossa YYYYMMDD (määrittelyn start_date / stop_date)."""
    return None if value is None or pd.isna(value) else pd.Timestamp(value).strftime("%Y%m%d")


def _time_part(value) -> str | None:
    """Aikaleiman kelloaika muodossa HHMMSS (määrittelyn start_time / stop_time)."""
    return None if value is None or pd.isna(value) else pd.Timestamp(value).strftime("%H%M%S")


def _duration_min(start, stop) -> float | None:
    """Jakson kesto minuutteina (stop - start). Puuttuva raja -> None."""
    if start is None or stop is None or pd.isna(start) or pd.isna(stop):
        return None
    return _rounded((pd.Timestamp(stop) - pd.Timestamp(start)).total_seconds() / 60)


# --- Tapahtumarivin runko ------------------------------------------------------------

def _base_event(event_type_id: int, config: dict,
                start_at, stop_at, program: str | None = None) -> dict:
    """Määrittelyn kohdan 3 yhteinen Machine Event -runko. Käyttämättömät kentät NULL.

    Jokainen tapahtuma alkaa tästä: kaikki DAGin kirjoittamat sarakkeet tyhjinä, ja
    päälle ne kentät jotka ovat yhteisiä kaikille tyypeille. Kutsuja täyttää loput
    omansa.

    Rajat tulevat laskennan UTC-aikajanalla ja muunnetaan TÄSSÄ paikalliseen aikaan:
    start_date/start_time ovat paikallista seinäkelloaikaa, ja start_at/stop_at
    kirjoitetaan aikavyöhykkeellisinä, jolloin kanta tallentaa oikean hetken.

    machine_event_id EI ole mukana: se on kohdekannassa bigserial, jonka kanta generoi.
    Riveillä ei siis ole DAGin puolelta ennustettavaa tunnusta, mikä ratkaisee sen
    miten replace_day kohdistaa poistonsa -- ks. postgres._paivan_poistoehto.
    """
    row = dict.fromkeys(MACHINE_EVENT_COLUMNS)
    row["event_type_id"] = event_type_id
    row["source_id"] = int(config["target"]["source_id"])
    row["workcenter_id"] = _workcenter_id(config)
    row["program_name"] = program
    start_at, stop_at = _to_local(start_at, config), _to_local(stop_at, config)
    row["start_at"] = start_at
    row["stop_at"] = stop_at
    row["start_date"] = _date_part(start_at)
    row["stop_date"] = _date_part(stop_at)
    row["start_time"] = _time_part(start_at)
    row["stop_time"] = _time_part(stop_at)
    row["duration_min"] = _duration_min(start_at, stop_at)
    return row


# --- Raakatason esikäsittely ---------------------------------------------------------

def _normalise_raw(raw: pd.DataFrame) -> pd.DataFrame:
    """Raakatason rivit laskentaa varten: aikaleimat, numerot ja tila-arvot.

    Lisää kaksi saraketta joita koko osa 5 käyttää:
        label   tila-arvo, jos se on tunnettu (STATES), muuten tyhjä
        end_at  rivin päättymishetki
    """
    df = raw.copy()
    if df.empty:
        df = pd.DataFrame(columns=EVENT_COLUMNS)

    # format="mixed" on pakollinen: tilarivit ovat tasaminuuteilla mutta kappalerivit
    # kantavat mikrosekunnit. Ilman tätä pandas päättelee muodon ensimmäisestä rivistä
    # ja coercaa jokaisen mikrosekunnillisen aikaleiman NaT:ksi.
    # utc=True pitää sarakkeen aikavyöhykkeellisenä myös kun rivejä ei ole lainkaan.
    df["time"] = pd.to_datetime(df["time"], errors="coerce", format="mixed", utc=True)
    for column in ["duration_min", "part_count", "part_delta", "cycle_min",
                   *CYCLE_DELTA_COLUMNS, *OVERRIDE_SIGNALS]:
        df[column] = pd.to_numeric(df.get(column), errors="coerce")

    df["label"] = df["state"].where(df["state"].isin(STATES))
    # Tilarivin loppu = alku + kesto; kappalerivi on hetkellinen.
    df["end_at"] = df["time"] + pd.to_timedelta(df["duration_min"].fillna(0), unit="m")
    return df.sort_values("time").reset_index(drop=True)


def _state_segment_frame(rows: pd.DataFrame) -> pd.DataFrame:
    """Tilajaksot leikkauslaskentaa varten (tyypin 10 kohdistus).

    Pelkät tilarivit kolmena sarakkeena (start_at, stop_at, label), jotta
    _overlap_minutes voi kysyä "kuinka monta minuuttia tätä tilaa osui tälle välille".
    """
    states = rows[rows["event_type"] == RAW_EVENT_STATE]
    return pd.DataFrame({
        "start_at": states["time"],
        "stop_at": states["end_at"],
        "label": states["label"],
    }).dropna(subset=["start_at", "stop_at"]).reset_index(drop=True)


def _overlap_minutes(segments: pd.DataFrame, start, stop, label: str) -> float:
    """Kuinka monta minuuttia annettua tilaa osui välille [start, stop].

    Kappaletapahtuma ei noudata tilajaksojen rajoja, joten sen sisään voi osua useita
    jaksoja ja jaksoja voi jäädä puoliksi ulkopuolelle. Jokainen jakso leikataan siksi
    välin rajoihin (clip) ja leikkaamatta jääneet pudotetaan pois (negatiivinen kesto).
    """
    if start is None or stop is None or pd.isna(start) or pd.isna(stop) or segments.empty:
        return 0.0
    selected = segments[segments["label"] == label]
    if selected.empty:
        return 0.0
    lower = selected["start_at"].clip(lower=start)
    upper = selected["stop_at"].clip(upper=stop)
    overlap = (upper - lower).dt.total_seconds() / 60
    return float(overlap[overlap > 0].sum())


# --- Tapahtumatyypit 11, 14 ja 16: tilajaksot ----------------------------------------

def _state_events(rows: pd.DataFrame, config: dict) -> list[dict]:
    """Tyypit 11, 14 ja 16: yksi rivi per tilajakso.

    Suorin käännös kaikista: yksi raakatason tilarivi = yksi tapahtuma. Tila ratkaisee
    tyypin -- työstö (tehokas tai tehoton) on 11, Idle on 14 ja Pois päältä on 16.
    Tunnuksen generoi kanta, joten tapahtumia ei numeroida täällä.
    """
    events = []

    for _, row in rows[rows["event_type"] == RAW_EVENT_STATE].iterrows():
        label, duration = row["label"], _num(row["duration_min"])
        if label is None:
            continue  # tuntematon tila-arvo, ei kuulu mihinkään tapahtumatyyppiin

        if label in (STATE_EFFICIENT, STATE_INEFFICIENT):
            event_type_id = EVENT_TYPE_CUTTING
        elif label == STATE_IDLE:
            event_type_id = EVENT_TYPE_IDLE
        elif label == STATE_POWER_OFF:
            event_type_id = EVENT_TYPE_POWER_OFF
        else:
            continue

        # Määrittelyn kohta 8 ei listaa POWER OFFille program_name-kenttää.
        program = None if event_type_id == EVENT_TYPE_POWER_OFF else row["program"]
        event = _base_event(event_type_id, config,
                            row["time"], row["end_at"], program)

        # Jakson koko kesto kirjautuu sille kentälle, jota tila edustaa. Muut ajan
        # kentät jäävät NULLiksi -- yksi jakso on aina yhtä tilaa.
        if event_type_id == EVENT_TYPE_CUTTING:
            efficient = duration if label == STATE_EFFICIENT else 0.0
            inefficient = duration if label == STATE_INEFFICIENT else 0.0
            event["efficient_production_min"] = _rounded(efficient)
            event["inefficient_production_min"] = _rounded(inefficient)
            event["machine_active_min"] = _rounded((efficient or 0.0) + (inefficient or 0.0))
        elif event_type_id == EVENT_TYPE_IDLE:
            event["idle_sum_min"] = _rounded(duration)
            # Jakso on kokonaan idleä, joten osuus on määritelmällisesti 100 %.
            event["idle_sum_pct"] = 1.0
        else:
            event["power_off_min"] = _rounded(duration)
            # power_off_pct on määritelty vain koontitapahtumassa (tyyppi 17).

        events.append(event)

    return events


# --- Tapahtumatyyppi 10: kappaleet ---------------------------------------------------

def _part_change_events(rows: pd.DataFrame, segments: pd.DataFrame,
                        config: dict) -> list[dict]:
    """Tyyppi 10: yksi rivi per kappalelaskuritapahtuma.

    Kertoo yhden kappaleen tarinan: kauanko sen valmistumiseen kului kelloaikaa
    (cycle_min), paljonko siitä oli koneistusta (operating) ja paljonko lastuamista
    (cutting). Erotus kelloajan ja koneistuksen välillä on vaihtotyötä.
    """
    events = []
    part_rows = rows[rows["event_type"] == RAW_EVENT_PART]

    for _, row in part_rows.iterrows():
        cycle = _num(row["cycle_min"])
        operating = _num(row[OPERATING_DELTA])
        cutting = _num(row[CUTTING_DELTA])

        # TULKINTA 1: määrittely sanoo "työkirjan nykyisen säännön mukainen johdettu
        # alku/loppu" mutta ei kerro sääntöä. Laskuri kirjautuu kappaleen valmistuessa,
        # joten jakso päättyy siihen ja alkaa sykliajan verran aiemmin.
        stop_at = row["time"]
        start_at = stop_at - pd.Timedelta(minutes=cycle) if cycle is not None else None

        event = _base_event(EVENT_TYPE_PART_CHANGE, config,
                            start_at, stop_at, row["program"])

        event["unit_time_min"] = _rounded(cycle)
        event["unit_cycle_time_min"] = _rounded(cycle)
        event["unit_processing_time_min"] = _rounded(operating)
        event["produced_qty_machine"] = _num(row["part_delta"])
        event["part_counter"] = _num(row["part_count"])
        event["cutting_time_min"] = _rounded(cutting)
        # Koneistusaika josta lastuaminen puuttuu: pikaliikkeet, työkalunvaihdot yms.
        event["non_cutting_time"] = _rounded(_positive_gap(operating, cutting))
        event["feed_override"] = _num(row[SIGNAL_FEED_OVERRIDE])
        event["spindle_override"] = _num(row[SIGNAL_SPINDLE_OVERRIDE])

        # Kappaleen jakso ei noudata tilajaksojen rajoja, joten tehokas ja tehoton aika
        # lasketaan leikkauksena tilajaksoihin -- ei summaamalla kokonaisia jaksoja.
        efficient = _overlap_minutes(segments, start_at, stop_at, STATE_EFFICIENT)
        inefficient = _overlap_minutes(segments, start_at, stop_at, STATE_INEFFICIENT)
        event["efficient_production_min"] = _rounded(efficient)
        event["inefficient_production_min"] = _rounded(inefficient)
        event["machine_active_min"] = _rounded(efficient + inefficient)
        event["machine_active_pct"] = _share(efficient + inefficient, cycle)

        # Vaihtotyöaika: se osa sykliajasta jolloin ohjelma ei ollut ajossa.
        part_change = _positive_gap(cycle, operating)
        event["part_change_time_min"] = _rounded(part_change)
        event["part_change_time_pct"] = _share(part_change, cycle)
        # TULKINTA 3: määrittely kutsuu tätä "vaihtotyöajan rinnakkaismittariksi"
        # kertomatta miten se eroaa part_change_time_min:istä. Sama arvo kunnes
        # työkirja kertoo toisin.
        event["idle_sum_min"] = _rounded(part_change)
        event["idle_sum_pct"] = _share(part_change, cycle)

        events.append(event)

    return events


# --- Tapahtumatyyppi 8: tuotantojaksot -----------------------------------------------

def _production_periods(rows: pd.DataFrame, break_min: float) -> list[pd.DataFrame]:
    """Tyypin 8 jaksotus: saman ohjelman yhtenäiset tuotantojaksot.

    Käydään raakatason rivit aikajärjestyksessä ja kerätään ne jaksoiksi. Uusi jakso
    alkaa aina kun jokin katkaisee edellisen; kerätyt rivit ovat rivi-indeksejä, joista
    lopuksi poimitaan osataulukot.

    TULKINTA 2: määrittely sanoo "ohjelman vaihtuminen tai selkeä tuotantokatkos
    aloittaa uuden jakson" ja kehottaa tarkistamaan kynnyksen työkirjasta.
    Tässä katkaisevat: ohjelman vaihtuminen, puuttuva ohjelma (UNAVAILABLE ei ole
    tuotanto-ohjelma), Pois päältä -jakso, ja break_min minuuttia pidempi tauko.
    """
    periods: list[list[int]] = []
    current: list[int] | None = None     # kerättävä jakso, None = ei jaksoa kesken
    previous_program = None
    previous_end = None                  # edellisen mukaan otetun rivin päättymishetki

    for index, row in rows.iterrows():
        program = row["program"]
        blank = program is None or pd.isna(program) or program == UNAVAILABLE_PROGRAM

        # Ei tuotanto-ohjelmaa tai kone sammuksissa: jakso päättyy eikä tämä rivi kuulu
        # mihinkään jaksoon. Päättymishetki jää muistiin tauon mittaamista varten.
        if blank or row["label"] == STATE_POWER_OFF:
            current, previous_program, previous_end = None, None, row["end_at"]
            continue

        # Kuinka pitkä tauko edellisen rivin päättymisestä tähän riviin.
        gap = 0.0
        if previous_end is not None and pd.notna(previous_end):
            gap = (row["time"] - previous_end).total_seconds() / 60

        if current is None or program != previous_program or gap > break_min:
            current = []
            periods.append(current)

        current.append(index)
        previous_program = program
        # Rivit voivat mennä päällekkäin (kappalerivi tilajakson sisällä), joten
        # päättymishetki vain kasvaa -- muuten tauko laskettaisiin liian pitkäksi.
        previous_end = row["end_at"] if pd.isna(previous_end) else max(previous_end, row["end_at"])

    return [rows.loc[indexes] for indexes in periods if indexes]


def _production_events(rows: pd.DataFrame, config: dict) -> list[dict]:
    """Tyyppi 8: ohjelmakohtainen tuotantojakso aggregaatteineen.

    Karkein raekoko tilajakson ja vuorokauden välissä: yksi rivi kertoo, miten yhden
    ohjelman yhtenäinen ajo sujui -- kauanko se kesti, montako kappaletta valmistui ja
    miten aika jakautui työstön, idlen ja vaihtotyön kesken.
    """
    events = []
    break_min = float(config["calculation"]["production_break_min"])

    for period in _production_periods(rows, break_min):
        # Jakson kesto kellosta: ensimmäisen rivin alusta viimeisen rivin loppuun.
        # Kirjautuu duration_min-kenttään (_base_event) ja on osuuksien nimittäjä.
        start_at = period["time"].min()
        stop_at = period["end_at"].max()
        period_min = (stop_at - start_at).total_seconds() / 60

        event = _base_event(EVENT_TYPE_PRODUCTION, config,
                            start_at, stop_at, period["program"].iloc[0])

        # Ajan jakautuminen tiloittain: summataan jakson tilarivien kestot.
        states = period["label"]
        durations = period["duration_min"]
        efficient = _total(durations[states == STATE_EFFICIENT])
        inefficient = _total(durations[states == STATE_INEFFICIENT])
        idle = _total(durations[states == STATE_IDLE])

        # Kappalekohtaiset arvot jakson sisältä. Tilariveillä nämä ovat None, joten
        # listat sisältävät myös tyhjiä -- _total ja _average ohittavat ne.
        cycles = [_num(value) for value in period["cycle_min"]]
        operatings = [_num(value) for value in period[OPERATING_DELTA]]
        cuttings = [_num(value) for value in period[CUTTING_DELTA]]

        # Kappaleajat ovat KESKIARVOJA jakson yli, eivät summia: kenttä kuvaa yhtä
        # tyypillistä kappaletta, ei koko jaksoa. unit_time_min on sykliaika per
        # kappale, kuten tyypissä 10 -- jakson kesto on duration_min.
        event["unit_time_min"] = _average(cycles)
        event["unit_cycle_time_min"] = _average(cycles)
        event["unit_processing_time_min"] = _average(operatings)
        event["produced_qty_machine"] = _total([_num(v) for v in period["part_delta"]])
        event["efficient_production_min"] = _rounded(efficient)
        event["inefficient_production_min"] = _rounded(inefficient)
        event["cutting_time_min"] = _rounded(_total(cuttings))
        event["non_cutting_time"] = _rounded(
            sum(_positive_gap(o, c) for o, c in zip(operatings, cuttings)))
        event["machine_active_min"] = _rounded(efficient + inefficient)
        event["machine_active_pct"] = _share(efficient + inefficient, period_min)

        # Vaihtotyöaika kappale kerrallaan ja vasta sitten summana -- kokonaissummien
        # erotus antaisi eri tuloksen, koska osalta kappaleista arvo puuttuu.
        part_change = sum(_positive_gap(c, o) for c, o in zip(cycles, operatings))
        event["part_change_time_min"] = _rounded(part_change)
        event["part_change_time_pct"] = _share(part_change, period_min)
        event["idle_sum_min"] = _rounded(idle)
        event["idle_sum_pct"] = _share(idle, period_min)

        events.append(event)

    return events


# --- Tapahtumatyyppi 17: vuorokauden koonti ------------------------------------------

def _day_event(rows: pd.DataFrame, period_start, period_end, config: dict) -> dict:
    """Tyyppi 17: vuorokauden koonti suoraan raakadatasta, ei summaamalla muita tasoja.

    Koonti lasketaan kaikista vuorokauden raakariveistä, jolloin siihen tulee mukaan
    myös aika joka jäi tuotantojaksojen ulkopuolelle (sammutukset, ohjelmattomat pätkät).
    Muiden tyyppien summaaminen ei tuottaisi samaa lukua -- eikä ole tarkoituskaan.

    Palauttaa yhden rivin, aina: myös täysin hiljainen vuorokausi saa koontinsa.
    """
    day_minutes = (period_end - period_start).total_seconds() / 60
    # Kaikki osuudet lasketaan tätä vasten, joten poikkeama vääristäisi ne. Odotettu
    # ikkuna on kokonainen paikallinen vuorokausi -- kellonsiirtopäivänä 23 tai 25 h,
    # mikä ei ole poikkeama. Ajoa ei kaadeta, mutta muu ikkuna kirjataan lokiin jotta
    # se löytyy jälkikäteen.
    day_start, day_end = _local_day(period_start, config)
    if (period_start, period_end) != (day_start, day_end):
        _log("machine_day_window_poikkeaa", day_minutes=round(day_minutes, 2),
             odotettu=round((day_end - day_start).total_seconds() / 60, 2))

    event = _base_event(EVENT_TYPE_DAY, config, period_start, period_end)

    states = rows["label"]
    durations = rows["duration_min"]
    efficient = _total(durations[states == STATE_EFFICIENT])
    inefficient = _total(durations[states == STATE_INEFFICIENT])
    idle = _total(durations[states == STATE_IDLE])
    power_off = _total(durations[states == STATE_POWER_OFF])

    cycles = [_num(value) for value in rows["cycle_min"]]
    operatings = [_num(value) for value in rows[OPERATING_DELTA]]
    cuttings = [_num(value) for value in rows[CUTTING_DELTA]]

    event["produced_qty_machine"] = _total([_num(v) for v in rows["part_delta"]])
    event["efficient_production_min"] = _rounded(efficient)
    event["inefficient_production_min"] = _rounded(inefficient)
    event["cutting_time_min"] = _rounded(_total(cuttings))
    event["non_cutting_time"] = _rounded(
        sum(_positive_gap(o, c) for o, c in zip(operatings, cuttings)))
    event["feed_override"] = _average([_num(v) for v in rows[SIGNAL_FEED_OVERRIDE]])
    event["spindle_override"] = _average([_num(v) for v in rows[SIGNAL_SPINDLE_OVERRIDE]])
    event["machine_active_min"] = _rounded(efficient + inefficient)
    event["machine_active_pct"] = _share(efficient + inefficient, day_minutes)
    # Määrittelyn kohta 9: asetustilaa ei erotella nykyisessä raakadatassa.
    event["setup_time_min"] = 0.0
    event["setup_time_pct"] = 0.0

    part_change = sum(_positive_gap(c, o) for c, o in zip(cycles, operatings))
    event["part_change_time_min"] = _rounded(part_change)
    event["part_change_time_pct"] = _share(part_change, day_minutes)
    event["idle_sum_min"] = _rounded(idle)
    event["idle_sum_pct"] = _share(idle, day_minutes)
    event["power_off_min"] = _rounded(power_off)
    event["power_off_pct"] = _share(power_off, day_minutes)

    return event


# --- Kaikkien tyyppien kokoaminen ----------------------------------------------------

def _build_machine_events(raw: pd.DataFrame, period_start, period_end,
                          config: dict) -> pd.DataFrame:
    """Johtaa kaikki kuusi tapahtumatyyppiä raakatason riveistä.

    Sisääntulopiste: raakatason CSV-rivit sisään, valmis kohdetaulun mittainen
    DataFrame ulos.

    Eri raekokojen tapahtumia ei summata keskenään (määrittelyn kohta 10): sama
    aika esiintyy tarkoituksella tilatapahtumassa, tuotantojaksossa ja koonnissa.
    """
    rows = _normalise_raw(raw)
    segments = _state_segment_frame(rows)

    # Jokainen tyyppi lasketaan itsenäisesti samoista riveistä ja liitetään yhteen.
    events = (
        _production_events(rows, config)
        + _part_change_events(rows, segments, config)
        + _state_events(rows, config)
        + [_day_event(rows, period_start, period_end, config)]
    )

    frame = pd.DataFrame(events, columns=MACHINE_EVENT_COLUMNS)
    return frame.sort_values(["event_type_id", "start_at"], na_position="last").reset_index(drop=True)
