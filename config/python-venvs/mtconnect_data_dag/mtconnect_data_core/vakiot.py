"""
vakiot.py -- laskennan vakiot, signaalit, kohdetaulun muoto ja asetusten oletukset.

Kaikki mitä muutetaan harvoin: signaalien loogiset nimet, tila-arvot, tapahtumatyyppien
numerot, kohdetaulun sarakkeet ja asetusten oletusarvot. Tämä moduuli ei tuo mitään
muuta paketin moduulia, joten se on riippuvuuspuun juuri.

Polut ratkaistaan paketin sijainnista: asetustiedosto ja .env ovat paketin ISÄNTÄ-
hakemistossa venv/:n ja requirements.txt:n rinnalla, eivät paketin sisällä.
"""

from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path

# Paketin hakemisto ja sen yläpuolinen juuri, jossa asetukset, .env ja venv sijaitsevat.
# Näin koko hakemiston voi siirtää sellaisenaan toiseen ympäristöön.
PAKETTI_DIR = Path(__file__).resolve().parent
JUURI_DIR = PAKETTI_DIR.parent

# Aikakatkaisut ovat Airflow'n asia ja määritellään DAG-tiedostossa mtconnect_data_dag.py.


# --- Signaalit -----------------------------------------------------------------------
# Laskennan käyttämät signaalit LOOGISILLA nimillä. Asetusten signals-osio kertoo, millä
# measurementilla ja nimellä kukin löytyy lähteestä, ja combine_and_validate_raw_data
# kääntää lähteen nimet näiksi. Laskenta ei siis tiedä keruun nimistä mitään, ja toisen
# keruun tai ohjaimen käyttöönotto on pelkkä asetusmuutos.
#
# Nimet ovat samalla signals-osion avaimet asetustiedostossa.
SIGNAL_RUNNING_TIME = "running_time"           # koneen päälläoloaika
SIGNAL_OPERATING_TIME = "operating_time"       # ohjelman ajossaoloaika
SIGNAL_CUTTING_TIME = "cutting_time"           # lastuavan työstön aika
SIGNAL_SPINDLE_TIME = "spindle_time"           # karan pyörimisaika
SIGNAL_FEED_OVERRIDE = "feed_override"         # syöttöarvon override (%)
SIGNAL_SPINDLE_OVERRIDE = "spindle_override"   # karan override (%)
SIGNAL_PART_COUNT = "part_count"               # kappalelaskuri
SIGNAL_PROGRAM = "program"                     # NC-ohjelman nimi


class SignalKind(Enum):
    """Signaalin laji. Ratkaisee miten signaali haetaan ja miten sitä käsitellään."""

    # Kumulatiivinen aikalaskuri. Yksittäinen lukema ei kerro mitään; tieto on lukemien
    # EROTUKSESSA. Haetaan minuuttimaksimeina ja muunnetaan sekunneiksi (unit).
    COUNTER = auto()
    # Asetusarvo joka kirjoitetaan vain muuttuessaan. Haetaan raakana, ja lisäksi
    # lämmittelyhaku noutaa viimeisimmän arvon ennen hakuikkunaa.
    STATE = auto()
    # Tapahtumalaskuri (kappaleet). Haetaan raakana. Ei lämmittelyä: hakuikkunan
    # ulkopuolinen lukema vääristäisi ensimmäisen erotuksen.
    EVENT = auto()


@dataclass(frozen=True)
class SignalSpec:
    """Signaalin laji, pakollisuus ja oletuslähde (TUOVA-ympäristön MTConnect-keruu)."""
    kind: SignalKind
    # Pakollinen signaali puuttuu haetusta datasta -> ajo kaatuu, koska laskenta
    # tuottaisi muuten oikean näköisiä mutta vääriä lukuja.
    required: bool
    measurement: str
    name: str


SIGNALS = {
    SIGNAL_RUNNING_TIME: SignalSpec(SignalKind.COUNTER, True, "mtc_metrics", "ptotalrunningtime"),
    SIGNAL_OPERATING_TIME: SignalSpec(SignalKind.COUNTER, True, "mtc_metrics", "ptotaloperatingtime"),
    SIGNAL_CUTTING_TIME: SignalSpec(SignalKind.COUNTER, True, "mtc_metrics", "ptotalcuttingtime"),
    SIGNAL_SPINDLE_TIME: SignalSpec(SignalKind.COUNTER, True, "mtc_metrics", "ptotalspindleruntime"),
    SIGNAL_FEED_OVERRIDE: SignalSpec(SignalKind.STATE, True, "mtc_metrics", "pfovr"),
    SIGNAL_SPINDLE_OVERRIDE: SignalSpec(SignalKind.STATE, False, "mtc_metrics", "s1ovr"),
    SIGNAL_PART_COUNT: SignalSpec(SignalKind.EVENT, True, "mtc_metrics", "ppartcount"),
    SIGNAL_PROGRAM: SignalSpec(SignalKind.STATE, True, "mtc_status", "pprogram"),
}

# Laskurit tilaluokittelua varten: kasvu minuutissa kertoo koneen tilan.
COUNTER_SIGNALS = [signal for signal, spec in SIGNALS.items() if spec.kind is SignalKind.COUNTER]

# Laskurien yksiköt kertoimina sekunneiksi. Laskenta käyttää aina sekunteja, joten
# muunnos tehdään heti haun jälkeen (signals.<laskuri>.unit).
COUNTER_UNITS = {"ms": 0.001, "s": 1.0, "min": 60.0, "h": 3600.0}
DEFAULT_COUNTER_UNIT = "s"

# Samat laskurit kappalekohtaiseen laskentaan: paljonko YHDEN kappaleen aikana ehti
# kulua koneistusta. Näin wall-clock-sykliajasta erottuu se osa, jonka kone oikeasti
# työskenteli. Arvo on raakatason sarakkeen etuliite (esim. "cutting_delta_min").
PIECE_CYCLE_COUNTERS = {
    SIGNAL_CUTTING_TIME: "cutting",
    SIGNAL_OPERATING_TIME: "operating",
    SIGNAL_SPINDLE_TIME: "spindle",
}


def cycle_delta_column(signal: str) -> str:
    """Raakatason sarake, johon laskurin kasvu yhden kappaleen aikana kirjataan."""
    return f"{PIECE_CYCLE_COUNTERS[signal]}_delta_min"


CYCLE_DELTA_COLUMNS = [cycle_delta_column(signal) for signal in PIECE_CYCLE_COUNTERS]

# Käyttäjän säätämät nopeusprosentit: syöttöarvon ja karan override.
# Näistä ei oteta erotusta -- arvo luetaan sellaisenaan.
OVERRIDE_SIGNALS = [SIGNAL_FEED_OVERRIDE, SIGNAL_SPINDLE_OVERRIDE]

# Yhdistetyn datan sarake, jossa on signaalin looginen nimi. Lähteen oma nimitagi
# (source.item_tag) käännetään tähän combine_and_validate_raw_data-taskissa.
SIGNAL_COLUMN = "signal"


# --- Influx-haut ---------------------------------------------------------------------
# Haut johdetaan signals-osiosta: yksi haku per measurement ja hakutapa.
QUERY_RAW = "raw"            # mittaukset sellaisenaan
QUERY_COUNTER = "counter"    # laskurit minuuttimaksimeina
QUERY_WARMUP = "warmup"      # tilamaisten signaalien viimeisin arvo ennen hakuikkunaa

# Haun rooli ajossa: varsinainen haku ei saa olla tyhjä, lämmittelyhaku saa.
JOB_MAIN = "main"
JOB_WARMUP = "warmup"

# max/1min tasoittaa laskurien näytteenoton vaihtelun muuttamatta laskurin arvoa.
COUNTER_WINDOW = "1m"
COUNTER_AGG_FN = "max"
# Lämmittelyhaku: kentän viimeisin arvo ennen hakuikkunaa.
WARMUP_AGG_FN = "last"

# Flux-hakujen sallitut aggregointifunktiot. Nimi menee kyselymerkkijonoon
# sellaisenaan, joten se tarkistetaan listaa vasten eikä päästetä läpi mitä tahansa.
VALID_AGG_FNS = {"mean", "median", "sum", "min", "max", "last", "first", "count"}


# --- Raakataso ("data v1") -----------------------------------------------------------
# Raakatason rivilajit (event_type-sarake).
RAW_EVENT_STATE = "state_change"   # yhtenäinen jakso samassa tilassa (alku + kesto)
RAW_EVENT_PART = "part"            # hetki jolloin kappalelaskuri kasvoi

# Raakatason CSV-sarakkeet. Aikaikkuna ja aikaleimat: ks. AIKALEIMOJEN KORJAUS
# DAG-tiedoston docstringissä.
EVENT_COLUMNS = (
    ["time", "event_type", "program", "state", "duration_min", "part_count", "part_delta", "cycle_min"]
    + CYCLE_DELTA_COLUMNS
    + OVERRIDE_SIGNALS
)


# --- Koneen tilat --------------------------------------------------------------------
# Tilaluokittelun arvot. Kirjoitetaan raakatason state-sarakkeeseen sellaisenaan.
STATE_EFFICIENT = "Efficient cutting"
STATE_INEFFICIENT = "Inefficient cutting"
STATE_IDLE = "Idle"
STATE_POWER_OFF = "Power off"
STATES = (STATE_EFFICIENT, STATE_INEFFICIENT, STATE_IDLE, STATE_POWER_OFF)

# Koneen ilmoittama arvo silloin kun ohjelmatietoa ei ole. Ei siis tuotanto-ohjelma,
# vaan tieto sen puuttumisesta -- katkaisee tuotantojakson (tyyppi 8).
UNAVAILABLE_PROGRAM = "UNAVAILABLE"


# --- Tapahtumatyypit -----------------------------------------------------------------
# Määrittelyn tapahtumatyyppien numerot. Nämä menevät kohdetaulun event_type_id-
# sarakkeeseen ja ratkaisevat, mitkä kentät rivillä ylipäätään täytetään.
EVENT_TYPE_PRODUCTION = 8      # tuotantojakso, saman ohjelman yhtenäinen pätkä
EVENT_TYPE_PART_CHANGE = 10    # yksi kappalelaskurin tapahtuma
EVENT_TYPE_CUTTING = 11        # yksi työstöjakso (tehokas tai tehoton)
EVENT_TYPE_IDLE = 14           # kone päällä mutta ei työstössä
EVENT_TYPE_POWER_OFF = 16      # sammutusjakso
EVENT_TYPE_DAY = 17            # vuorokauden koonti


# --- Kohdetaulu ----------------------------------------------------------------------
# Sarakkeet jotka DAG KIRJOITTAA: kaikkien tapahtumatyyppien kenttien unioni. Kentän
# käyttö riippuu event_type_id:stä, käyttämättömät jäävät NULLiksi.
#
# HUOM: machine_event_id EI ole tässä listassa. Se on tuotantokannassa bigserial, eli
# kanta generoi sen -- DAG ei saa täyttää sitä. Siksi tunnus ei myöskään ole
# ennustettava, eikä siihen voi kohdistaa poistoa tai ON CONFLICT -ehtoa.
#
# DAG EI LUO TAULUA -- kanta omistaa skeemansa. Tämä lista on se, mitä kohdetaulussa on
# oltava; _verify_target_table tarkistaa sen ennen jokaista kirjoitusta.
#
# Nämä ovat DAGIN SISÄISIÄ nimiä. Kohdetaulun nimet voivat poiketa -- ks.
# TARGET_COLUMN_MAP heti listan jälkeen.
MACHINE_EVENT_COLUMNS = [
    "event_type_id",
    "source_id",
    "workcenter_id",
    "program_name",
    "start_date",
    "stop_date",
    "start_time",
    "stop_time",
    "start_at",
    "stop_at",
    # Tapahtumarivin kesto (stop_at - start_at). Sama nimi kuin raakatason
    # EVENT_COLUMNS-sarakkeella, mutta eri taso: tapahtumarivit ovat omia dictejään.
    "duration_min",
    "unit_time_min",
    "unit_cycle_time_min",
    "unit_processing_time_min",
    "produced_qty_machine",
    "part_counter",
    "efficient_production_min",
    "inefficient_production_min",
    "cutting_time_min",
    "non_cutting_time",
    "feed_override",
    "spindle_override",
    "machine_active_min",
    "machine_active_pct",
    "setup_time_min",
    "setup_time_pct",
    "part_change_time_min",
    "part_change_time_pct",
    "idle_sum_min",
    "idle_sum_pct",
    "power_off_min",
    "power_off_pct",
]

# Kohdetaulun sarakenimet siltä osin kuin ne poikkeavat DAGin sisäisistä nimistä.
#
# Tuotannon tietovarasto on tähtimalli, jossa taulu on fact_machine_events ja jakson
# loppua kuvaavat kentät on nimetty end_*. DAGin laskenta käyttää nimiä stop_*, ja ne
# ovat siellä muutakin kuin sarakenimiä: paikallisia muuttujia, DataFrame-sarakkeita ja
# dict-avaimia kymmenissä kohdissa. Lisäksi konetapahtumat.py käyttää nimeä end_at jo
# tilasegmenteille, joten sisäinen uudelleennimeäminen aiheuttaisi nimikonfliktin.
#
# Siksi käännös tehdään VASTA kirjoitusvaiheessa: postgres.py mappaa nimet INSERTiin,
# poistoehtoon ja taulutarkistukseen. Laskenta ei tiedä kohdetaulun nimistä mitään.
#
# Tuotannon tietomalli on ainoa totuus -- mappaus on kiinteä eikä konfiguroitava.
TARGET_COLUMN_MAP = {
    "stop_date": "end_date",
    "stop_time": "end_time",
    "stop_at": "end_at",
    "non_cutting_time": "non_cutting_time_min",
}

# Sisäiset nimet kohdetaulun niminä, samassa järjestyksessä kuin MACHINE_EVENT_COLUMNS.
# Järjestys ratkaisee: INSERTin sarakelista ja arvotuplet rakennetaan tästä.
TARGET_COLUMNS = [TARGET_COLUMN_MAP.get(column, column) for column in MACHINE_EVENT_COLUMNS]


def target_column(column: str) -> str:
    """DAGin sisäinen sarakenimi kohdetaulun nimenä."""
    return TARGET_COLUMN_MAP.get(column, column)


# --- Asetusten oletukset -------------------------------------------------------------
# Asetukset luetaan YAML-tiedostosta tämän moduulin vierestä. Tiedosto on ainoa totuus:
# Airflow-Variableja ei käytetä. Osioiden ja avainten nimet vastaavat alla olevaa
# DEFAULT_CONFIGia, ja koodissa arvo näkyy muodossa config["calculation"]["threshold"].
#
# Molemmat polut ratkaistaan tämän tiedoston sijainnista, joten ympäristömuuttujia ei
# tarvita ja koko hakemiston voi siirtää sellaisenaan toiseen ympäristöön.
CONFIG_FILE = JUURI_DIR / "mtconnect_data_config.yaml"

# Tunnukset. Ei versionhallintaan -- .env.example kertoo mitkä avaimet tarvitaan.
ENV_FILE = JUURI_DIR / ".env"


def _default_signal(spec: SignalSpec) -> dict:
    """Signaalin oletuslähde asetusmuodossa. Laskureilla myös yksikkö."""
    source = {"measurement": spec.measurement, "name": spec.name}
    if spec.kind is SignalKind.COUNTER:
        source["unit"] = DEFAULT_COUNTER_UNIT
    return source


# DEFAULT_CONFIG on kaksi asiaa yhdessä:
#   1. Oletusarvot. Puuttuva osio tai avain täydentyy täältä, jolloin DAG pysyy
#      ajokelpoisena vaikka asetustiedostosta puuttuisi uusi asetus.
#   2. SALLITTUJEN AVAINTEN LUETTELO. Avain jota ei ole täällä ohitetaan ja
#      kirjataan lokiin varoituksena -- koodi ei kuitenkaan lukisi sitä.
#
# Poikkeus on machines: koneella ei ole järkevää oletusta, joten tyhjä lista kaataa
# ajon. Väärä kone hiljaa oletuksena olisi pahempi kuin kaatuminen.
DEFAULT_CONFIG = {
    # Osoitteet ja tunnukset ovat .env-tiedostossa, eivät täällä. Tänne jäävät vain
    # yhteyden tekniset asetukset, jotka eivät ole salaisia.
    "connection": {
        "verify_ssl": False,
        "timeout_ms": 60000,
    },
    "target": {
        "schema": "public",
        "table": "fact_machine_events",
        # replace_day = päivän rivit poistetaan ja kirjoitetaan uudelleen, joten sama
        # päivä voidaan ajaa monta kertaa ilman kaksoiskappaleita. Ks. VALID_WRITE_MODES.
        "write_mode": "replace_day",
        # 2 = MACHINE API dim_source-taulussa. Ks. mtconnect_data_config.yaml.
        "source_id": 2,
    },
    # Kone(et) joiden dataa haetaan. Ks. MACHINE_KEYS ja MAX_MACHINES.
    "machines": [],
    "time": {
        # KELLOVIRHE: Influxin aikaleimat ovat tämän verran jäljessä UTC:stä. Ei sisällä
        # aikavyöhykettä -- se on alla erikseen. Ks. AIKALEIMOJEN KORJAUS DAG-tiedoston
        # mtconnect_data_dag.py docstringissä. Oletus 0 = aikaleimat ovat oikein.
        "clock_error_hours": 0,
        # Paikallinen aika IANA-nimenä. Määrää vuorokauden rajat sekä kentät
        # start_date/start_time; kesä- ja talviaika tulevat tästä automaattisesti.
        "timezone": "Europe/Helsinki",
        # 12 h: tyyppien 8 ja 10 jaksot on saatava kokonaisina vuorokauden rajan yli.
        "lookback_hours": 12,
        "lookahead_hours": 12,
        # Kuinka kaukaa taaksepäin etsitään kentän viimeisin tunnettu arvo, jos se ei
        # muutu hakuikkunan aikana. Ilman tätä esim. ohjelman nimi puuttuisi kokonaan
        # vuorokaudelta, jonka aikana ohjelmaa ei vaihdettu.
        "warmup_days": 7,
    },
    # Lähteen rakenne: millä tagilla Influx nimeää mittauksen ja millä laitteen.
    "source": {
        "item_tag": "name",
        "device_tag": "device",
    },
    # Looginen signaali -> lähteen measurement ja nimi. Ks. SIGNALS.
    "signals": {signal: _default_signal(spec) for signal, spec in SIGNALS.items()},
    "calculation": {
        # Tilaluokittelun aikaresoluutio: yksi tilarivi per tämän mittainen ikkuna.
        "aggr": "1min",
        # Laskurin kasvu sekunteina ikkunaa kohti, jota suurempi tulkitaan käynniksi.
        # Ei nolla, koska laskurit heittelevät hieman koneen ollessa pysähdyksissä.
        "threshold": 5.0,
        # Syöttöarvon override (%) jonka täyttyessä työstö on "tehokasta".
        "override_threshold": 50.0,
        # TULKINTA 2: tätä pidempi tauko katkaisee tuotantojakson (tyyppi 8).
        "production_break_min": 30.0,
        # Uskottavuusrajat sykliajalle: lyhyempi on mittausartefakti ja pidempi
        # käytännössä tauko kappaleiden välissä. Rajojen ulkopuolinen jää tyhjäksi.
        "min_cycle_min": 0.1,
        "max_cycle_min": 120.0,
    },
    "files": {
        # Levyjuuri: alle syntyy output/ (partitiot ja kooste) ja staging/ (välidata).
        # Oletuksena venv-hakemiston data/, sama kuin asetustiedoston esimerkissä.
        "data_root": str(JUURI_DIR / "data"),
        "partition_prefix": "state-events",     # -> output/state-events_2026-09-08.csv
        "combined_file": "state-events-all.csv",
        # Montako tuoreinta partitiota koostetiedostoon otetaan mukaan.
        "merge_window_days": 90,
        # Tätä vanhemmat staging-hakemistot siivotaan pois. Epäonnistuneiden ajojen
        # välidata jää siis tutkittavaksi viikoksi.
        "staging_max_age_days": 7,
    },
}

# Koneen avaimet machines-listassa. workcenter_id on dim_workcenterin tunnus, joka
# kopioidaan jokaiselle tapahtumariville. device rajaa Influx-haun laitteeseen
# (source.device_tag); ilman sitä haetaan koko bucketista.
MACHINE_REQUIRED_KEYS = {"workcenter_id"}
MACHINE_OPTIONAL_KEYS = {"device"}

# Montako konetta yksi asennus ajaa. Useamman koneen tuki on seuraava kehitysvaihe;
# siihen asti yksi kone per asennus, ja konelista on valmiiksi oikeassa muodossa.
MAX_MACHINES = 1

# Signaalin avaimet signals-osiossa. unit vain laskureille (ks. COUNTER_UNITS).
SIGNAL_KEYS = {"measurement", "name"}
COUNTER_SIGNAL_KEYS = SIGNAL_KEYS | {"unit"}

# Vanhan, suomenkielisen asetustiedoston osiot ja niiden korvaajat. Vanha tiedosto
# KAATAA ajon: se ei saa jäädä hiljaa käyttöön oletusarvoilla, koska oletukset ovat
# muuttuneet (mm. kellovirhe ja kone). Ohje siirtoon: README, "Upgrading".
REMOVED_CONFIG_SECTIONS = {
    "yhteydet": "connection",
    "kohde": "target (target_schema -> schema, target_table -> table) ja "
             "machines (workcenter_id)",
    "aikaikkuna": "time",
    "laskenta": "calculation",
    "tiedostot": "files",
    "haut": "signals (haut johdetaan nyt signaaleista)",
}

# Poistetut asetusavaimet ja ohje tilalle tulleista. Tuntematon avain yleensä vain
# ohitetaan, mutta nämä KAATAVAT ajon: vanha asetustiedosto ei saa jäädä hiljaa
# käyttöön, kun avaimen merkitys on muuttunut.
REMOVED_CONFIG_KEYS = {
    "time": {
        "time_offset_hours": (
            "korvattu avaimilla clock_error_hours (Influxin kellovirhe, TUOVAssa 6) ja "
            "timezone (Europe/Helsinki)"
        ),
    },
}

# Asetusavaimet joiden arvoa ei kirjoiteta lokiin. Asetustiedostossa ei enää ole
# salaisuuksia -- ne ovat .env:ssä -- mutta joukko jätetään paikalleen, jotta
# myöhemmin lisättävä arka avain voidaan vaimentaa yhdestä paikasta.
SECRET_KEYS: set[str] = set()

# .env-avaimet joiden arvo on salainen. _log ei koskaan kirjoita näitä, ja
# _env_settings palauttaa lokitettavan yhteenvedon ilman niitä.
SECRET_ENV_KEYS = {"INFLUX_TOKEN", "PG_DSN", "PG_DSN_FALLBACK"}

# Kirjoitustavat kohdetauluun:
#   replace_day  poista päivän rivit ja kirjoita uudelleen (oletus, idempotentti)
#   append       lisää vain -- kaksoiskappaleet jäävät, käytä harkiten
#
# "upsert" POISTETTU: se kohdisti ON CONFLICT -ehdon machine_event_id:hen, mutta kanta
# generoi tunnuksen (bigserial) eikä DAG enää täytä sitä. Rivi ei siis voi törmätä
# olemassa olevaan, joten upsert olisi vain hitaampi append. Tietomallissa ei ole muuta
# luonnollista uniikkiavainta, johon ehdon voisi kohdistaa.
VALID_WRITE_MODES = {"replace_day", "append"}
