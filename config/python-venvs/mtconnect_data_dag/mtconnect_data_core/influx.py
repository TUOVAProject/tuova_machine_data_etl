"""
influx.py -- Flux-hakujen suunnittelu, kyselyn rakennus ja ajo sekä tuloksen käännös
signaaleiksi.

Haut johdetaan asetusten signals-osiosta (_signal_queries), joten käyttäjä kertoo vain
millä nimellä kukin signaali löytyy -- ei kirjoita kyselyitä.

Tulos palautetaan "pitkänä" DataFramena, jossa yksi rivi on yksi mittaus: time,
measurement, field, value ja lähteen omat tagit (mm. source.item_tag, jolla mittaus
nimetään). _to_signals kääntää lähteen nimet loogisiksi signaaleiksi ja laskurit
sekunneiksi -- siitä eteenpäin laskenta ei tiedä lähteestä mitään.

Osoite, token ja organisaatio luetaan .env-tiedostosta (apurit._env_settings).
"""

import pandas as pd
import urllib3
from influxdb_client import InfluxDBClient

from .apurit import _env_required, _env_settings, _machine, _safe_name
from .vakiot import (
    COUNTER_AGG_FN,
    COUNTER_UNITS,
    COUNTER_WINDOW,
    QUERY_COUNTER,
    QUERY_RAW,
    QUERY_WARMUP,
    SIGNAL_COLUMN,
    SIGNALS,
    SignalKind,
    VALID_AGG_FNS,
)

# Influx-palvelimella on itse allekirjoitettu sertifikaatti (connection.verify_ssl =
# false), joten jokainen haku kirjoittaisi muuten varoituksen task-lokiin.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _query_modes(kind: SignalKind) -> list[str]:
    """Millä hakutavoilla signaalin laji haetaan. Ks. vakiot.SignalKind."""
    if kind is SignalKind.COUNTER:
        return [QUERY_COUNTER]
    if kind is SignalKind.STATE:
        return [QUERY_RAW, QUERY_WARMUP]
    return [QUERY_RAW]


def _signal_queries(config: dict) -> list[dict]:
    """Haut johdettuna signals-osiosta: yksi haku per hakutapa ja measurement.

    Palauttaa hakujen rungot ilman aikaikkunaa (vaiheet.plan_influx_queries_for_interval
    lisää sen). Järjestys on vakio, jotta lokit ja staging-tiedostot ovat samat joka
    ajolla. Avain nimeää haun lokissa ja staging-tiedostossa.
    """
    groups: dict[tuple[str, str], list[str]] = {}
    for signal, spec in SIGNALS.items():
        source = config["signals"][signal]
        for mode in _query_modes(spec.kind):
            names = groups.setdefault((mode, source["measurement"]), [])
            if source["name"] not in names:
                names.append(source["name"])

    queries = []
    for (mode, measurement), names in sorted(groups.items()):
        query = {"key": f"{mode}_{_safe_name(measurement)}", "mode": mode,
                 "measurement": measurement, "fields": names}
        if mode == QUERY_COUNTER:
            query.update(window=COUNTER_WINDOW, agg_fn=COUNTER_AGG_FN)
        queries.append(query)
    return queries


def _flux_string(value: str) -> str:
    """Merkkijono Flux-literaaliksi. Kenoviiva ja lainausmerkki escapoidaan, jottei
    asetustiedoston nimi voi katkaista kyselyä."""
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _build_query(bucket: str, start: str, stop: str, measurement: str,
                 fields: list[str] | None, window: str | None, agg_fn: str | None,
                 item_tag: str, device_tag: str | None = None,
                 device: str | None = None) -> str:
    """Rakentaa Flux-hakulausekkeen yhdelle kyselylle.

    Ilman aggregointia palautetaan mittaukset sellaisenaan. Aggregoinnilla (window +
    agg_fn) Influx tiivistää ne ikkunoittain -- tätä käytetään sekä laskurien
    minuuttimaksimeihin että lämmittelyhakuun (fn: last).

    item_tag on tagi, jolla lähde nimeää mittauksen (esim. "name"). device rajaa haun
    yhteen laitteeseen tagilla device_tag; ilman sitä haetaan koko bucketista.

    Flux-aikaliteraalit kirjoitetaan ilman lainausmerkkejä, toisin kuin merkkijonot.
    """
    use_agg = bool(agg_fn) and agg_fn.lower() not in ("", "none")
    if use_agg and agg_fn not in VALID_AGG_FNS:
        raise ValueError(f"Tuntematon aggregointifunktio '{agg_fn}'. Sallitut: {sorted(VALID_AGG_FNS)}")

    query = (
        f'from(bucket: {_flux_string(bucket)})'
        f' |> range(start: {start}, stop: {stop})'
        f' |> filter(fn: (r) => r["_measurement"] == {_flux_string(measurement)})'
    )
    if device:
        query += f' |> filter(fn: (r) => r[{_flux_string(device_tag)}] == {_flux_string(device)})'
    if fields:
        tag = _flux_string(item_tag)
        conditions = " or ".join(f'r[{tag}] == {_flux_string(field)}' for field in fields)
        query += f' |> filter(fn: (r) => {conditions})'
    if use_agg:
        query += (
            f' |> aggregateWindow(every: {window}, fn: {agg_fn}, createEmpty: false)'
            f' |> yield(name: "{agg_fn}")'
        )
    else:
        query += ' |> yield(name: "raw")'
    return query


def _job_query(config: dict, job: dict, bucket: str) -> str:
    """Suunnitellun haun Flux-kysely asetusten lähde- ja laitetiedoilla."""
    return _build_query(
        bucket=bucket, start=job["start"], stop=job["stop"],
        measurement=job["measurement"], fields=job["fields"],
        window=job.get("window"), agg_fn=job.get("agg_fn"),
        item_tag=config["source"]["item_tag"],
        device_tag=config["source"]["device_tag"],
        device=_machine(config).get("device"),
    )


def _query_influx(config: dict, query: str) -> pd.DataFrame:
    """Suorittaa kyselyn ja palauttaa rivit pitkänä DataFramena.

    Osoite, token ja organisaatio luetaan .env-tiedostosta; verify_ssl ja timeout_ms
    tulevat asetustiedostosta, koska ne eivät ole salaisuuksia. Puuttuvasta avaimesta
    kaadutaan heti nimeltä mainiten, koska influxdb_clientin oma virhe ei kerro mikä
    tieto puuttui.
    """
    env = _env_settings()
    url = _env_required(env, "INFLUX_URL").rstrip("/")

    with InfluxDBClient(url=url,
                        token=_env_required(env, "INFLUX_TOKEN"),
                        org=_env_required(env, "INFLUX_ORG"),
                        verify_ssl=bool(config["connection"]["verify_ssl"]),
                        timeout=int(config["connection"]["timeout_ms"])) as client:
        tables = client.query_api().query(query=query)

    # Influxin oma vastausrakenne litistetään riveiksi. Kiinteät kentät otetaan
    # nimettyinä; loput tagit (mm. source.item_tag, jolla mittaus nimetään) kopioidaan
    # sellaisenaan. skip-joukko jättää pois Influxin sisäiset kentät, jotka on jo luettu
    # nimettyihin sarakkeisiin.
    skip = {"_time", "_measurement", "_field", "_value", "result", "table"}
    rows = []
    for table in tables:
        for record in table.records:
            row = {
                "time": record.get_time(),
                "measurement": record.get_measurement(),
                "field": record.get_field(),
                "value": record.get_value(),
            }
            for key, value in record.values.items():
                if key not in skip and key not in row:
                    row[key] = value
            rows.append(row)
    return pd.DataFrame(rows)


def _bucket(config: dict) -> str:
    """Haettava Influx-bucket. Luetaan .env-tiedostosta, ei asetustiedostosta."""
    return _env_required(_env_settings(), "INFLUX_BUCKET")


def _to_signals(config: dict, combined: pd.DataFrame) -> pd.DataFrame:
    """Lähteen nimet loogisiksi signaaleiksi ja laskurit sekunneiksi.

    Lisää sarakkeen SIGNAL_COLUMN. Signaali tunnistetaan parista (measurement, nimi),
    koska sama nimi voi esiintyä eri measurementeissa. Rivit joille signaalia ei löydy
    pudotetaan -- haku rajattiin jo signaalien nimiin, joten niitä ei pitäisi olla.

    Laskurien yksikkö (signals.<laskuri>.unit) muunnetaan tässä sekunneiksi, jolloin
    laskennan kynnysarvot ovat aina sekunteja lähteestä riippumatta.
    """
    item_tag = config["source"]["item_tag"]
    if item_tag not in combined.columns:
        raise ValueError(
            f"Haetusta datasta puuttuu tagi '{item_tag}' (source.item_tag). Tarkista millä "
            "tagilla keruu nimeää mittaukset.")

    by_source = {
        (config["signals"][signal]["measurement"], config["signals"][signal]["name"]): signal
        for signal in SIGNALS
    }
    keys = zip(combined["measurement"], combined[item_tag])
    combined = combined.assign(**{SIGNAL_COLUMN: [by_source.get(key) for key in keys]})
    combined = combined.dropna(subset=[SIGNAL_COLUMN]).reset_index(drop=True)

    for signal, spec in SIGNALS.items():
        factor = COUNTER_UNITS.get(config["signals"][signal].get("unit"), 1.0)
        if spec.kind is SignalKind.COUNTER and factor != 1.0:
            rows = combined[SIGNAL_COLUMN] == signal
            combined.loc[rows, "value"] = pd.to_numeric(combined.loc[rows, "value"], errors="coerce") * factor
    return combined


def _missing_signals(config: dict, combined: pd.DataFrame) -> list[str]:
    """Pakolliset signaalit joita haetussa datassa ei ole lainkaan, lähteen nimin."""
    present = set(combined[SIGNAL_COLUMN].unique())
    return [
        f"{signal} ({config['signals'][signal]['measurement']}/{config['signals'][signal]['name']})"
        for signal, spec in SIGNALS.items()
        if spec.required and signal not in present
    ]
