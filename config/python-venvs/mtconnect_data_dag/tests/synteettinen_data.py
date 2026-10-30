"""
synteettinen_data.py -- synteettinen konedata testiympäristön InfluxDB:hen ja testeihin.

Tuottaa pisteitä samassa muodossa kuin MTConnect-keruu kirjoittaa ja DAG lukee:
measurement, tagit (laite, komponentti, elementti, nimi) sekä kenttä "value" (numero)
tai "state" (teksti). Signaalien nimet, measurementit ja yksiköt, tagien nimet,
kellovirhe ja aikavyöhyke luetaan DAGin omista asetuksista, joten generaattori ei voi
erota DAGista.

Jokainen vuorokausi noudattaa samaa ohjelmaa (_paivaohjelma), jossa esiintyy jokainen
tapahtumatyyppi: sammutus (16), idle (14), tehokas ja tehoton työstö (11), kappaleet
(10), kaksi ohjelmaa eli kaksi tuotantojaksoa (8) ja vuorokauden koonti (17).

Kaksi käyttötapaa:

    Komentorivi    line protocol stdoutiin, yhteenveto stderriin (dev/seed.sh):
                   venv/bin/python tests/synteettinen_data.py --date 2026-10-14 --days 1
    Moduuli        generoi(config, paiva, paivia) palauttaa pisteet listana
                   (tests/test_regressio.py)

Tulos on deterministinen: sama päivä tuottaa aina samat pisteet, joten uudelleenlataus
ei luo kaksoiskappaleita ja regressiotesti voi verrata tulosta tallennettuun.
"""

import argparse
import bisect
import contextlib
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# Core-paketti on tämän hakemiston yläpuolella (venv-hakemisto).
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mtconnect_data_core import vakiot  # noqa: E402
from mtconnect_data_core.vakiot import (  # noqa: E402
    SIGNAL_CUTTING_TIME,
    SIGNAL_FEED_OVERRIDE,
    SIGNAL_OPERATING_TIME,
    SIGNAL_PART_COUNT,
    SIGNAL_PROGRAM,
    SIGNAL_RUNNING_TIME,
    SIGNAL_SPINDLE_OVERRIDE,
    SIGNAL_SPINDLE_TIME,
)

# --- Laite ja tagit ---------------------------------------------------------------------
# Laite, jos asetuksissa ei ole machines[0].device -- DAG ei silloin rajaa laitetta.
DEVICE = "SIM-0001"
# DAG ei käytä komponentti- ja elementtitageja, mutta keruu kirjoittaa ne aina.
COMPONENT_TAG, COMPONENT = "component", "path"
ELEMENT_TAG = "element"
# Keruun kenttä ja elementti arvon tyypin mukaan: numerot "value"/Sample, tekstit
# "state"/Event.
NUMBER_FIELD, TEXT_FIELD = "value", "state"
NUMBER_ELEMENT, TEXT_ELEMENT = "Sample", "Event"

# --- Koneen tilat ja vuorokauden ohjelma -------------------------------------------------
OFF, IDLE, CUT = "off", "idle", "cut"

# Laskurien kasvu sekunteina minuutissa tilan mukaan. DAG tulkitsee yli 5 s/min kasvun
# käynniksi.
COUNTER_RATE = {
    SIGNAL_RUNNING_TIME: {IDLE: 60, CUT: 60},
    SIGNAL_OPERATING_TIME: {CUT: 60},
    SIGNAL_CUTTING_TIME: {CUT: 45},
    SIGNAL_SPINDLE_TIME: {CUT: 50},
}
COUNTER_START_S = 1_000_000          # laskurien lähtölukema, ei merkitystä laskennalle

SPINDLE_OVERRIDE_PCT = 100.0         # kirjoitetaan aina kun kone käynnistyy


@dataclass(frozen=True)
class Jakso:
    """Vuorokauden ohjelman yksi jakso. Kestää seuraavan jakson alkuun asti.

    ohjelma=None tarkoittaa, että edellinen ohjelma jatkuu. Uuden ohjelman lataus
    nollaa kappalelaskurin, kuten oikeassakin koneessa.
    """
    alku: time                       # paikallinen kellonaika
    tila: str
    ohjelma: str | None = None
    pfovr: float | None = None       # syöttöoverride %, DAG: >= 50 tehokas
    kappalevali_min: int | None = None


def _paivaohjelma(unavailable: str) -> list[Jakso]:
    """Yksi vuorokausi. Tehoton työstö: pfovr alle DAGin override_thresholdin (50)."""
    return [
        Jakso(time(0, 0), OFF, ohjelma=unavailable),
        Jakso(time(6, 0), IDLE, ohjelma="SIM-PROGRAM-A"),
        Jakso(time(6, 30), CUT, pfovr=100.0, kappalevali_min=12),
        Jakso(time(10, 0), IDLE),
        Jakso(time(10, 15), CUT, pfovr=30.0, kappalevali_min=15),
        Jakso(time(12, 0), IDLE),
        Jakso(time(12, 45), CUT, ohjelma="SIM-PROGRAM-B", pfovr=100.0, kappalevali_min=10),
        Jakso(time(15, 30), IDLE),
        Jakso(time(16, 0), OFF, ohjelma=unavailable),
    ]


# --- Asetukset DAGin core-moduulista ------------------------------------------------------

def _lataa_asetukset() -> dict:
    """Lukee asennetut asetukset. Asetusloki ohjataan stderriin, koska stdout on
    varattu line protocolille."""
    import mtconnect_data_core as core

    with contextlib.redirect_stdout(sys.stderr):
        return core.load_config()


# --- Pisteet ----------------------------------------------------------------------------

@dataclass(frozen=True)
class Piste:
    """Yksi mittaus Influxin (virheellisellä) aikajanalla, kuten keruu sen kirjoittaa."""
    measurement: str
    value: float | str
    at: datetime                     # UTC miinus kellovirhe
    tags: dict[str, str]

    @property
    def is_text(self) -> bool:
        return isinstance(self.value, str)

    @property
    def field_key(self) -> str:
        return TEXT_FIELD if self.is_text else NUMBER_FIELD

    def line_protocol(self) -> str:
        tags = ",".join(f"{key}={value}" for key, value in self.tags.items())
        value = f'"{self.value}"' if self.is_text else float(self.value)
        return f"{self.measurement},{tags} {self.field_key}={value} {int(self.at.timestamp())}"


class Keraaja:
    """Kerää signaalien pisteet listaan lähteen nimillä ja yksiköillä, Influxin
    (virheellisellä) aikajanalla."""

    def __init__(self, config: dict):
        self.signals = config["signals"]
        self.item_tag = config["source"]["item_tag"]
        self.device_tags = {config["source"]["device_tag"]: config["machines"][0].get("device") or DEVICE}
        self.clock_error = timedelta(hours=float(config["time"]["clock_error_hours"]))
        self.pisteet: list[Piste] = []
        self.kaytetyt: set[str] = set()

    def piste(self, signal: str, value, at: datetime) -> None:
        source = self.signals[signal]
        # Laskurit ovat simuloinnissa sekunteja; lähde kirjoittaa ne omassa yksikössään.
        if "unit" in source:
            value = value / vakiot.COUNTER_UNITS[source["unit"]]
        element = TEXT_ELEMENT if isinstance(value, str) else NUMBER_ELEMENT
        tags = {**self.device_tags, COMPONENT_TAG: COMPONENT, ELEMENT_TAG: element,
                self.item_tag: source["name"]}
        # Influxin kello on clock_error_hours jäljessä: oikea hetki miinus virhe.
        self.pisteet.append(Piste(source["measurement"], value, at - self.clock_error, tags))
        self.kaytetyt.add(signal)


# --- Simulointi -------------------------------------------------------------------------

def _simuloi(alku_utc: datetime, loppu_utc: datetime, tz: ZoneInfo,
             ohjelma: list[Jakso], unavailable: str, keraaja: Keraaja) -> None:
    """Käy aikavälin läpi minuutti kerrallaan ja kirjoittaa pisteet muutoksina,
    kuten keruu: laskurit kun ne kasvavat, tilat kun ne vaihtuvat."""
    alut = [jakso.alku for jakso in ohjelma]
    lukemat = {signal: float(COUNTER_START_S) for signal in COUNTER_RATE}
    edellinen_tila, edellinen_ohjelma, edellinen_avain = None, None, None
    kappaleet, tyosto_min = 0, 0
    ensimmainen = True

    t = alku_utc
    while t < loppu_utc:
        paikallinen = t.astimezone(tz)
        indeksi = bisect.bisect_right(alut, paikallinen.time()) - 1
        jakso = ohjelma[indeksi]
        tila = jakso.tila

        # Jakson vaihtuminen: tilakentät kirjoitetaan vain muuttuessaan.
        avain = (paikallinen.date(), indeksi)
        if avain != edellinen_avain:
            if jakso.ohjelma is not None and jakso.ohjelma != edellinen_ohjelma:
                keraaja.piste(SIGNAL_PROGRAM, jakso.ohjelma, t)
                edellinen_ohjelma = jakso.ohjelma
                if jakso.ohjelma != unavailable:
                    kappaleet = 0
                    keraaja.piste(SIGNAL_PART_COUNT, kappaleet, t)
            if edellinen_tila in (None, OFF) and tila != OFF:
                keraaja.piste(SIGNAL_SPINDLE_OVERRIDE, SPINDLE_OVERRIDE_PCT, t)
            if jakso.pfovr is not None:
                keraaja.piste(SIGNAL_FEED_OVERRIDE, jakso.pfovr, t)
            edellinen_avain, edellinen_tila, tyosto_min = avain, tila, 0

        # Laskurit: lähtölukema kerran, sen jälkeen vain kasvaessaan.
        for signal, kasvut in COUNTER_RATE.items():
            kasvu = kasvut.get(tila, 0)
            lukemat[signal] += kasvu
            if kasvu or ensimmainen:
                keraaja.piste(signal, lukemat[signal], t)

        # Kappale valmistuu tasaisin välein työstön aikana.
        if tila == CUT and jakso.kappalevali_min:
            tyosto_min += 1
            if tyosto_min % jakso.kappalevali_min == 0:
                kappaleet += 1
                keraaja.piste(SIGNAL_PART_COUNT, kappaleet, t)

        ensimmainen = False
        t += timedelta(minutes=1)


def _aikavali(config: dict, paiva: date, paivia: int) -> tuple[datetime, datetime]:
    """Generoitava aikaväli UTC:nä. Päivää ennen ja jälkeen mukana, koska DAG hakee
    lookback/lookahead-tunnit vuorokauden ulkopuolelta."""
    tz = ZoneInfo(config["time"]["timezone"])
    alku = datetime.combine(paiva - timedelta(days=1), time(0), tz)
    loppu = datetime.combine(paiva + timedelta(days=paivia + 1), time(0), tz)
    return alku.astimezone(timezone.utc), loppu.astimezone(timezone.utc)


def generoi(config: dict, paiva: date, paivia: int = 1) -> list[Piste]:
    """Pisteet päiville paiva .. paiva + paivia - 1 (ja päivä kummallakin puolella)."""
    tz = ZoneInfo(config["time"]["timezone"])
    alku, loppu = _aikavali(config, paiva, paivia)

    keraaja = Keraaja(config)
    _simuloi(alku, loppu, tz, _paivaohjelma(vakiot.UNAVAILABLE_PROGRAM),
             vakiot.UNAVAILABLE_PROGRAM, keraaja)

    puuttuvat = {signal for signal, spec in vakiot.SIGNALS.items() if spec.required} - keraaja.kaytetyt
    if puuttuvat:
        raise ValueError(f"Generaattori ei tuota pakollisia signaaleja: {sorted(puuttuvat)}")
    return keraaja.pisteet


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="ensimmäinen päivä YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=1, help="montako vuorokautta")
    args = parser.parse_args()

    config = _lataa_asetukset()
    pisteet = generoi(config, args.date, args.days)
    for piste in pisteet:
        print(piste.line_protocol())

    alku, loppu = _aikavali(config, args.date, args.days)
    print(f"Generoitu {len(pisteet)} pistettä, {args.date} + {args.days} vrk "
          f"(aikaväli {alku:%Y-%m-%d %H:%M} -> {loppu:%Y-%m-%d %H:%M} UTC, "
          f"kellovirhe {config['time']['clock_error_hours']} h)", file=sys.stderr)


if __name__ == "__main__":
    main()
