"""
asetukset.py -- asetustiedoston luku, oletusten täydennys ja tarkistukset.

Asetukset luetaan YAML-tiedostosta vakiot.CONFIG_FILE. Tiedosto on ainoa totuus, ja
DEFAULT_CONFIG on sekä oletusarvot että sallittujen avainten luettelo. Kaikki tarkistukset
tehdään tässä ennen ensimmäistäkään hakua, jotta virheellinen asetus kaatuu heti
load_config-taskissa selkeällä viestillä eikä vasta kesken laskennan.

Riippuu vakioista ja apureista.
"""

import copy
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml

from .apurit import _log, _zone
from .vakiot import (
    CONFIG_FILE,
    COUNTER_SIGNAL_KEYS,
    COUNTER_UNITS,
    DEFAULT_CONFIG,
    DEFAULT_COUNTER_UNIT,
    MACHINE_OPTIONAL_KEYS,
    MACHINE_REQUIRED_KEYS,
    MAX_MACHINES,
    REMOVED_CONFIG_KEYS,
    REMOVED_CONFIG_SECTIONS,
    SECRET_KEYS,
    SIGNAL_KEYS,
    SIGNALS,
    SignalKind,
    VALID_WRITE_MODES,
)

# Mistä arvo tuli -- lokiin, jotta kirjoitusvirhe näkyy siinä että arvo tuli oletuksesta.
FROM_FILE = "tiedosto"
FROM_DEFAULT = "oletus"


def _read_yaml() -> dict:
    """Asetustiedosto sanakirjana. Puuttuva tai muu kuin YAML-objekti kaataa ajon."""
    if not CONFIG_FILE.exists():
        raise ValueError(
            f"Asetustiedostoa ei ole: {CONFIG_FILE}. Kopioi "
            "mtconnect_data_config.example.yaml -> mtconnect_data_config.yaml ja "
            "täytä vähintään machines. Tarkista myös että config-kansio on mountattu.")

    raw_config = yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8")) or {}
    if not isinstance(raw_config, dict):
        raise ValueError(
            f"Asetustiedoston {CONFIG_FILE} pitää olla YAML-objekti, "
            f"oli {type(raw_config).__name__}.")
    return raw_config


def _reject_old_format(raw_config: dict) -> None:
    """Vanha, suomenkielinen asetustiedosto kaatuu ohjeen kanssa.

    Sitä ei voi ajaa oletuksilla, koska osa oletuksista muuttui samalla (kellovirhe 0,
    kone pakollinen) -- hiljainen ajo kirjoittaisi vääriä aikoja tai väärälle koneelle.
    """
    old = sorted(set(raw_config) & set(REMOVED_CONFIG_SECTIONS))
    if old:
        mapping = "; ".join(f"{section} -> {REMOVED_CONFIG_SECTIONS[section]}" for section in old)
        raise ValueError(
            f"Asetustiedosto {CONFIG_FILE} on vanhassa muodossa. Osiot on nimetty "
            f"uudelleen: {mapping}. Siirrä arvot uuteen pohjaan "
            "mtconnect_data_config.example.yaml -- ohje README:ssä kohdassa "
            "'Upgrading the configuration'.")


def _section_value(section: str, defaults, raw):
    """Yhden osion arvo: tiedoston arvot oletusten päällä. Palauttaa (arvo, lähteet,
    tuntemattomat avaimet).

    Oletukset kopioidaan, jottei ajon asetuksiin tehty muutos (esim. signaalin
    oletusyksikkö) muuta DEFAULT_CONFIGia.
    """
    defaults = copy.deepcopy(defaults)
    if isinstance(defaults, list):
        # Lista (machines) on aina kokonainen: ei täydennetä alkioittain.
        if raw is not None and not isinstance(raw, list):
            raise ValueError(f"Asetusosion '{section}' pitää olla YAML-lista, oli {type(raw).__name__}.")
        value = defaults if raw is None else raw
        return value, (FROM_DEFAULT if raw is None else FROM_FILE), []

    if raw is None:
        raw = {}
    elif not isinstance(raw, dict):
        raise ValueError(
            f"Asetusosion '{section}' pitää olla YAML-objekti, oli {type(raw).__name__}.")

    removed = sorted(set(raw) & set(REMOVED_CONFIG_KEYS.get(section, {})))
    if removed:
        guides = "; ".join(f"{section}.{key}: {REMOVED_CONFIG_KEYS[section][key]}" for key in removed)
        raise ValueError(f"Asetustiedostossa {CONFIG_FILE} on poistettu avain -- {guides}.")

    # Tuntematon avain ei kaada ajoa, mutta se ohitetaan: DEFAULT_CONFIG on
    # sallittujen avainten luettelo, eikä koodi lukisi avainta josta se ei tiedä.
    unknown = sorted(set(raw) - set(defaults))
    known = {key: value for key, value in raw.items() if key not in unknown}
    sources = {
        key: (FROM_FILE if key in known else FROM_DEFAULT)
        for key in sorted(defaults)
        if key not in SECRET_KEYS
    }
    return {**defaults, **known}, sources, [f"{section}.{key}" for key in unknown]


def _check_machines(machines: list) -> None:
    """Konelista: 1..MAX_MACHINES konetta, jokaisella workcenter_id."""
    if not machines:
        raise ValueError(
            "Osiossa machines ei ole yhtään konetta. Lisää kone, esimerkiksi:\n"
            "machines:\n  - workcenter_id: 1      # koneen tunnus dim_workcenter-taulussa")
    if len(machines) > MAX_MACHINES:
        raise ValueError(
            f"Osiossa machines on {len(machines)} konetta, mutta asennus tukee "
            f"toistaiseksi {MAX_MACHINES}. Tee jokaiselle koneelle oma asennus.")

    allowed = MACHINE_REQUIRED_KEYS | MACHINE_OPTIONAL_KEYS
    for index, machine in enumerate(machines):
        where = f"machines[{index}]"
        if not isinstance(machine, dict):
            raise ValueError(f"{where} pitää olla YAML-objekti, oli {type(machine).__name__}.")
        unknown = sorted(set(machine) - allowed)
        if unknown:
            raise ValueError(f"{where}: tuntemattomat avaimet {unknown}. Sallitut: {sorted(allowed)}.")
        missing = sorted(key for key in MACHINE_REQUIRED_KEYS if machine.get(key) in (None, ""))
        if missing:
            raise ValueError(f"{where}: puuttuu {', '.join(missing)}.")


def _check_signals(signals: dict) -> None:
    """Jokaisella signaalilla measurement ja nimi; laskureilla tunnettu yksikkö.

    Täydentää laskureille oletusyksikön, jotta laskenta voi lukea sen suoraan.
    """
    for signal, spec in SIGNALS.items():
        where = f"signals.{signal}"
        source = signals[signal]
        if not isinstance(source, dict):
            raise ValueError(f"{where} pitää olla YAML-objekti, oli {type(source).__name__}.")

        counter = spec.kind is SignalKind.COUNTER
        allowed = COUNTER_SIGNAL_KEYS if counter else SIGNAL_KEYS
        unknown = sorted(set(source) - allowed)
        if unknown:
            raise ValueError(f"{where}: tuntemattomat avaimet {unknown}. Sallitut: {sorted(allowed)}.")
        missing = sorted(key for key in SIGNAL_KEYS if not source.get(key))
        if missing:
            raise ValueError(f"{where}: puuttuu {', '.join(missing)}.")

        if counter:
            source.setdefault("unit", DEFAULT_COUNTER_UNIT)
            if source["unit"] not in COUNTER_UNITS:
                raise ValueError(
                    f"{where}.unit: tuntematon yksikkö '{source['unit']}'. "
                    f"Sallitut: {', '.join(COUNTER_UNITS)}.")


def _check_values(config: dict) -> None:
    """Yksittäisten arvojen tarkistukset, jotka kaatavat ajon heti alussa."""
    write_mode = config["target"]["write_mode"]
    if write_mode not in VALID_WRITE_MODES:
        raise ValueError(
            f"Tuntematon write_mode '{write_mode}'. Sallitut: {', '.join(sorted(VALID_WRITE_MODES))}")

    try:
        ZoneInfo(_zone(config))
    except (ZoneInfoNotFoundError, ValueError, TypeError) as error:
        raise ValueError(
            f"Tuntematon aikavyöhyke '{_zone(config)}' (time.timezone). "
            "Anna IANA-nimi, esimerkiksi Europe/Helsinki.") from error


def _load_config() -> dict:
    """Lukee asetukset, täydentää oletuksilla ja tarkistaa ne. Ks. moduulin docstring.

    Yksittäinen puuttuva osio tai avain täydentyy DEFAULT_CONFIGista -- näin DAG pysyy
    ajokelpoisena vaikka tiedostosta puuttuisi uusi asetus. Loki kertoo jokaisesta
    osiosta, mitkä avaimet tulivat tiedostosta ja mitkä oletuksesta, joten
    kirjoitusvirhe näkyy siinä että odotettu avain raportoidaan oletuksena.

    Tuntematon avain kirjautuu varoituksena mutta ei kaada ajoa. Poikkeus ovat vanhan
    muodon osiot ja poistetut avaimet, jotka kaatavat ajon -- vanha asetustiedosto ei
    saa jäädä hiljaa käyttöön.
    """
    raw_config = _read_yaml()
    _reject_old_format(raw_config)

    config, sources = {}, {}
    unknown = [section for section in sorted(raw_config) if section not in DEFAULT_CONFIG]
    for section, defaults in DEFAULT_CONFIG.items():
        config[section], sources[section], unknown_keys = _section_value(
            section, defaults, raw_config.get(section))
        unknown += unknown_keys

    _check_machines(config["machines"])
    _check_signals(config["signals"])
    _check_values(config)

    if unknown:
        _log("asetus_tuntematon_avain", avaimet=unknown,
             huom="ohitettu -- tarkista kirjoitusasu asetustiedostosta")
    _log("asetusten_lahteet", tiedosto=str(CONFIG_FILE), **sources)
    _log("asetukset", **{
        section: ({key: value for key, value in values.items() if key not in SECRET_KEYS}
                  if isinstance(values, dict) else values)
        for section, values in config.items()
    })
    return config
