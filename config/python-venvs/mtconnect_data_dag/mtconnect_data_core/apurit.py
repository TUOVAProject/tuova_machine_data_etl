"""
apurit.py -- polut levylle, ajon aikaikkuna ja aikamuunnokset, kone, tunnukset ja lokitus.

Näitä käyttävät kaikki muut moduulit. Riippuu vain vakioista.

"ctx" on DAG-puolelta tuleva tavallinen sanakirja, ei Airflow-konteksti -- ks. paketin
__init__.py:n docstring kohta AJOKONTEKSTI.
"""

import json
import re
from pathlib import Path

import pandas as pd
from dotenv import dotenv_values

from .vakiot import ENV_FILE


def _env_settings() -> dict:
    """Lukee tunnukset .env-tiedostosta.

    Venvissä ei ole Airflow'ta, joten Connectioneja ei voi lukea. Puuttuva tiedosto on
    asennusvirhe eikä tilanne jossa oletukset kelpaisivat, joten se kaataa ajon.

    Yksittäisten avainten puuttumista EI tarkisteta täällä, vaan siinä funktiossa joka
    avainta tarvitsee -- Influx-haku ei kaadu puuttuvaan PG_DSN:ään eikä toisin päin.
    """
    if not ENV_FILE.exists():
        raise ValueError(
            f"Tunnustiedostoa ei ole: {ENV_FILE}. Kopioi .env.example -> .env ja täytä "
            "arvot. Tiedoston oikeudet on syytä rajata: chmod 600."
        )
    return {key: value for key, value in dotenv_values(ENV_FILE).items() if value}


def _env_required(env: dict, key: str) -> str:
    """Pakollinen .env-arvo. Puuttuva kaataa ajon avaimen nimeltä maininten."""
    value = env.get(key)
    if not value:
        raise ValueError(
            f"Tunnustiedostosta {ENV_FILE} puuttuu avain {key} (tai se on tyhjä).")
    return value


def _safe_name(text: str) -> str:
    """Tiedosto- ja hakemistonimeksi kelpaava muoto: kaikki muu paitsi kirjaimet ja
    numerot korvataan alaviivalla (esim. run_id:n kaksoispisteet ja plusmerkit)."""
    return re.sub(r"[^0-9A-Za-z]+", "_", text)


def _output_dir(config: dict) -> Path:
    """Hakemisto pysyville tuloksille: päiväpartitiot ja koostetiedosto."""
    return Path(config["files"]["data_root"]) / "output"


def _staging_root(config: dict) -> Path:
    """Hakemisto ajokohtaiselle välidatalle. Alla yksi hakemisto per ajo."""
    return Path(config["files"]["data_root"]) / "staging"


def _staging_dir(config: dict, ctx, create: bool = True) -> Path:
    """Tämän ajon oma välidatahakemisto.

    Nimi johdetaan run_id:stä, jotta rinnakkaiset ajot eivät kirjoita toistensa
    päälle.
    """
    path = _staging_root(config) / _safe_name(ctx["run_id"])
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def _as_utc(value) -> pd.Timestamp:
    """ISO-merkkijono tai aikaleima UTC-aikaleimaksi.

    Aikaleimat kulkevat DAG-puolelta merkkijonoina, koska ne on sarjallistettava
    taskien välillä. Airflow antaa ne aikavyöhykkeellisinä, mutta tz-naiivi arvo
    tulkitaan varmuuden vuoksi UTC:ksi eikä paikalliseksi ajaksi.
    """
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def _data_interval(ctx) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Ajon oma aikaikkuna UTC:na. Tyhjä ikkuna keskeyttää ajon selkeällä virheellä."""
    start = _as_utc(ctx["data_interval_start"])
    end = _as_utc(ctx["data_interval_end"])
    if end <= start:
        raise ValueError(
            f"data_interval on tyhjä ({start} .. {end}). Valitse UI:ssa Ajopäivä-parametri, "
            "aja ajastettuna tai komennolla 'airflow backfill create'."
        )
    return start, end


def _clock_error(config: dict) -> pd.Timedelta:
    """Influxin kellovirhe: kuinka paljon sen aikaleimat ovat jäljessä UTC:stä."""
    return pd.Timedelta(hours=float(config["time"]["clock_error_hours"]))


def _zone(config: dict) -> str:
    """Paikallisen ajan aikavyöhyke IANA-nimenä (esim. Europe/Helsinki)."""
    return config["time"]["timezone"]


def _from_influx(value, config: dict):
    """Influxin aikaleima oikeaksi UTC-ajaksi. Toimii sarjalle ja yksittäiselle arvolle.

    Laskenta tehdään tällä aikajanalla: UTC:ssä ei ole kellonsiirtoja, joten kestot ja
    järjestys pysyvät oikeina myös kesä- ja talviajan vaihtuessa.
    """
    return value + _clock_error(config)


def _to_influx(value, config: dict):
    """Oikea UTC-aika Influxin omalle, virheelliselle aikajanalle (hakurajoja varten)."""
    return value - _clock_error(config)


def _to_local(value, config: dict):
    """UTC-aika paikalliseksi ajaksi. Aikavyöhyketieto säilyy, joten hetki ei muutu.

    Käytetään vain reunoilla: vuorokauden rajoissa ja ulos kirjoitettavissa kentissä.
    Puuttuva arvo (None/NaT) palautuu sellaisenaan.
    """
    if isinstance(value, pd.Series):
        return value.dt.tz_convert(_zone(config))
    if value is None or pd.isna(value):
        return value
    return pd.Timestamp(value).tz_convert(_zone(config))


def _local_midnight(day, config: dict) -> pd.Timestamp:
    """Paikallisen kalenteripäivän alku (klo 00:00) UTC-aikaleimana.

    Jos keskiyötä ei kellonsiirron takia ole, otetaan seuraava olemassa oleva hetki;
    kahdesti esiintyvästä keskiyöstä ensimmäinen. Suomessa siirto tehdään klo 3-4,
    joten kumpikaan ei toteudu -- säännöt ovat muita aikavyöhykkeitä varten.
    """
    naive = pd.Timestamp(day).tz_localize(None).normalize()
    local = naive.tz_localize(_zone(config), nonexistent="shift_forward", ambiguous=True)
    return local.tz_convert("UTC")


def _local_day(start, config: dict) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Sen paikallisen kalenteripäivän rajat UTC:nä, johon hetki start kuuluu.

    Loppu on SEURAAVAN päivän keskiyö, ei alku + 24 h: kellonsiirtopäivänä vuorokausi
    on 23 tai 25 tuntia.
    """
    day = _to_local(start, config).tz_localize(None).normalize()
    return _local_midnight(day, config), _local_midnight(day + pd.Timedelta(days=1), config)


def _resolve_interval(config: dict, ctx) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Laskettava vuorokausi UTC:nä: Ajopäivä-parametri jos annettu, muuten data_interval.

    Ajopäivä on PAIKALLINEN kalenteripäivä: ikkuna alkaa paikallisesta keskiyöstä ja
    päättyy seuraavaan. Kaikki ikkunaa tarvitsevat kohdat kutsuvat tätä -- myös ds
    johdetaan tästä, ei suoraan kontekstista. Muuten käsinajo kirjoittaisi rivit
    väärälle päivälle.
    """
    target = ctx.get("target_date")
    if target:
        return _local_day(_local_midnight(target, config), config)
    return _data_interval(ctx)


def _partition_ds(config: dict, ctx) -> str:
    """Partition päivätunnus (YYYY-MM-DD): ratkaistun ikkunan paikallinen päivä."""
    start, _ = _resolve_interval(config, ctx)
    return _to_local(start, config).strftime("%Y-%m-%d")


def _flux_time(value) -> str:
    """Aikaleima Flux-kyselyyn kelpaavaksi RFC3339-merkkijonoksi UTC:na."""
    return pd.Timestamp(value).tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def _target_period(config: dict, ctx) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Laskettava vuorokausi laskennan aikajanalla (oikea UTC, aikavyöhykkeellinen).

    Data on siirretty samalle aikajanalle combine_and_validate_raw_data-taskissa
    (_from_influx), joten rajoja voi verrata siihen suoraan. Vuorokausi alkaa
    paikallisesta keskiyöstä.
    """
    return _resolve_interval(config, ctx)


def _machine(config: dict) -> dict:
    """Kone jonka dataa ajo käsittelee (machines-listan ainoa alkio).

    Asennus ajaa toistaiseksi yhden koneen (vakiot.MAX_MACHINES), ja asetusten luku
    varmistaa että lista on juuri sen mittainen. Kun useampi kone tulee tuetuksi, kone
    valitaan ajokohtaisesti -- kutsujat käyttävät jo nyt tätä funktiota.
    """
    return config["machines"][0]


def _workcenter_id(config: dict) -> str:
    """Koneen työkeskustunnus (dim_workcenter) tekstinä, kuten se kirjoitetaan."""
    return str(_machine(config)["workcenter_id"])


def _log(event: str, **fields) -> None:
    """Strukturoitu tulosloki task-lokiin. Rivimäärät ovat tässä olennaisin osa:
    jos poistettuja on 500 ja lisättyjä 3, jotain on pielessä vaikka taski
    menisi vihreänä läpi."""
    print(json.dumps({"event": event, **fields}, ensure_ascii=False, default=str))
