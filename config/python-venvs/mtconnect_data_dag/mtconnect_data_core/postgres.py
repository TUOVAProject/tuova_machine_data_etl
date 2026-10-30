"""
postgres.py -- kohdetaulun tarkistus ja konetapahtumien kirjoitus.

DAG EI LUO TAULUA eikä skeemaa: kanta omistaa skeemansa, ja DAG kirjoittaa siihen mitä
on. Ennen kirjoitusta tarkistetaan että taulu on olemassa ja sisältää tarvittavat
sarakkeet.

Taulun ja skeeman nimet tulevat asetuksista, joten ne rakennetaan psycopg2:n
sql.Identifierillä eikä merkkijonoja yhdistelemällä -- siinä lainausmerkit ja escapetus
menevät oikein.

Tämä moduuli on ainoa paikka jossa DAGin sisäiset sarakenimet käännetään kohdetaulun
nimiksi (vakiot.TARGET_COLUMN_MAP). Laskenta ei tiedä käännöksestä mitään, joten
kohdetaulun nimeämiskäytäntö ei vuoda muualle koodiin.

Yhteysmerkkijono luetaan .env-tiedostosta, ei Airflow-Connectionista.
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from .apurit import _env_settings, _workcenter_id
from .vakiot import (
    ENV_FILE,
    EVENT_TYPE_PART_CHANGE,
    MACHINE_EVENT_COLUMNS,
    TARGET_COLUMNS,
    VALID_WRITE_MODES,
    target_column,
)


def _target_table(config: dict) -> sql.Identifier:
    """Kohdetaulu skeemoineen turvallisesti lainattuna SQL-tunnisteena."""
    return sql.Identifier(config["target"]["schema"], config["target"]["table"])


def _dsn_label(dsn: str) -> str:
    """Yhteysmerkkijonon lokikelpoinen muoto: host/tietokanta ilman tunnuksia.

    DSN sisältää salasanan, joten sitä ei saa kirjoittaa lokiin sellaisenaan. Lokista
    on silti nähtävä MIHIN kirjoitettiin -- se on koko lokirivin tarkoitus.
    """
    tail = dsn.rsplit("@", 1)[-1]           # tunnukset ovat ennen @-merkkiä
    return tail.split("?", 1)[0] or "tuntematon"


def _resolve_pg_dsn() -> tuple[str, bool]:
    """Käytettävä PostgreSQL-yhteysmerkkijono ja tieto siitä oliko se varakanta.

    Ensisijainen on PG_DSN. Jos sitä ei ole annettu, siirrytään varakantaan
    PG_DSN_FALLBACK. Näin DAG on ajettavissa ennen kuin ulkoisen kannan tunnukset ovat
    käytettävissä.

    Varallesiirtyminen on tarkoituksella äänekäs: kirjoittaminen varakantaan siinä
    uskossa että data menee tietovarastoon on ikävä yllätys.
    """
    env = _env_settings()
    if env.get("PG_DSN"):
        return env["PG_DSN"], False

    fallback = env.get("PG_DSN_FALLBACK")
    if not fallback:
        raise ValueError(
            f"Tunnustiedostosta {ENV_FILE} puuttuu PG_DSN eikä varakantaa ole "
            "määritetty (PG_DSN_FALLBACK on tyhjä).")
    return fallback, True


def _verify_target_table(cursor, config: dict, kohde: str) -> None:
    """Varmistaa että kohdetaulu on olemassa ja sisältää tarvittavat sarakkeet.

    DAG ei luo taulua -- kanta omistaa skeemansa. Ilman tätä tarkistusta puuttuva taulu
    tai sarake tulisi vastaan vasta psycopg2:n raakana virheenä kesken kirjoituksen,
    eikä virheestä näkisi mitä kohdetaulusta oikeastaan puuttui.

    Taulun ylimääräiset sarakkeet ovat sallittuja -- DAG kirjoittaa oman osajoukkonsa ja
    loput jäävät kannan huoleksi -- YHTÄ POIKKEUSTA lukuun ottamatta: ylimääräinen
    sarake joka on NOT NULL eikä sillä ole oletusarvoa tekee lisäyksestä mahdottoman.
    Sekin tarkistetaan, koska muuten vika ilmenisi vasta psycopg2:n NotNullViolationina
    kesken kirjoituksen.

    Vertailu tehdään KOHDETAULUN nimillä (TARGET_COLUMNS). Puuttuva sarake raportoidaan
    molemmilla nimillä, jotta lokista näkee kumpaa päätä korjata: kantaa vai mappausta.
    """
    schema = config["target"]["schema"]
    table = config["target"]["table"]

    # Skeema ja taulu ovat tässä ARVOJA eivätkä tunnisteita, joten ne menevät
    # parametreina -- ei sql.Identifierillä.
    cursor.execute(
        "SELECT column_name, is_nullable, column_default "
        "FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, table),
    )
    columns = cursor.fetchall()
    present = {row[0] for row in columns}

    if not present:
        raise ValueError(
            f"Kohdetaulua '{schema}.{table}' ei ole kannassa {kohde} "
            "(tai siihen ei ole lukuoikeutta). DAG ei luo taulua -- se on luotava "
            "kantaan erikseen tietomallin mukaisena."
        )

    missing = [
        # Mapattu sarake näytetään muodossa "end_date (sisäisesti stop_date)", muut
        # pelkällä nimellään.
        kohdenimi if kohdenimi == sisainen else f"{kohdenimi} (sisäisesti {sisainen})"
        for sisainen, kohdenimi in zip(MACHINE_EVENT_COLUMNS, TARGET_COLUMNS)
        if kohdenimi not in present
    ]
    if missing:
        raise ValueError(
            f"Kohdetaulusta '{schema}.{table}' puuttuu sarakkeita: {', '.join(missing)}. "
            "DAG ei muuta taulun rakennetta -- lisää sarakkeet kantaan, korjaa "
            "MACHINE_EVENT_COLUMNS vastaamaan tietomallia tai täydennä TARGET_COLUMN_MAP."
        )

    kirjoitettavat = set(TARGET_COLUMNS)
    pakolliset = [
        name for name, nullable, oletus in columns
        if name not in kirjoitettavat and nullable == "NO" and oletus is None
    ]
    if pakolliset:
        raise ValueError(
            f"Kohdetaulussa '{schema}.{table}' on sarakkeita joita DAG ei kirjoita mutta "
            f"jotka ovat NOT NULL ilman oletusarvoa: {', '.join(sorted(pakolliset))}. "
            "Lisäys kaatuisi. Poista sarake, anna sille oletusarvo, salli NULL -- tai "
            "lisää se MACHINE_EVENT_COLUMNS-listaan jos se kuuluu tietomalliin."
        )


def _paivan_poistoehto(config: dict, ds: str) -> tuple[sql.SQL, tuple]:
    """Rakentaa replace_dayn poistoehdon: mitkä rivit kuuluvat päivään ds.

    Kohdekannassa machine_event_id on bigserial, jonka kanta generoi, joten poistoa ei
    voi kohdistaa tunnukseen. Ehto rakennetaan tietomallin omista kentistä.

    Ratkaiseva havainto: raakatason rivit rajataan tavoitevuorokauteen niiden OMAN
    aikaleiman perusteella, ja se aikaleima päätyy tapahtumaan eri kenttään
    tapahtumatyypistä riippuen:

        tyyppi 10   kappalelaskurin hetki -> stop_at (ja stop_date)
                    start_at lasketaan taaksepäin sykliajan verran, joten se voi olla
                    edellisen vuorokauden puolella tai puuttua kokonaan (NULL).
        muut tyypit jakson alku -> start_at (ja start_date)
                    stop_at voi ylittää vuorokauden rajan.

    Ehto käyttää siis kummallekin sitä kenttää, joka vastaa partition rajausta. Näin se
    osuu tasan niihin riveihin jotka tämä DAG kirjoitti kyseiselle päivälle -- myös
    NULL-alkuisiin ja vuorokauden rajan ylittäviin -- eikä koskaan naapuripäivän
    riveihin.

    Rajaus workcenter_id:llä on tarpeen, jotta toisen koneen rivit eivät poistu kun
    sama päivä ajetaan toiselle työkeskukselle.

    Rajaus source_id:llä on tarpeen, koska samaan tauluun voi kirjoittaa myös muita
    prosesseja. Esimerkiksi sensoridata voi tulla samalle työkeskukselle samoina
    tapahtumatyyppeinä lähteellä 3 (SENSOR); tämä DAG on lähde 2 (MACHINE API). Ilman
    tätä rajausta päivän poisto veisi myös sensoririvit. Lähde on ainoa kenttä joka erottaa
    kirjoittajat: workcenter_id ja event_type_id ovat molemmilla samoja.

    Vertailu tehdään teksteinä start_date/stop_date-kenttiä vasten (YYYYMMDD), koska ne
    ovat tasa-arvovertailuja ja siten indeksoituvia -- toisin kuin aikaleima-alueet.
    Sekä ds että nämä kentät ovat PAIKALLISTA päivää (aikaikkuna.timezone), joten ne
    kuvaavat samaa vuorokautta myös kellonsiirtopäivinä.

    Ehdon sarakenimet ovat KOHDETAULUN nimiä: stop_date on tuotannossa end_date, joten
    se haetaan mappauksesta. Väärä nimi tässä ei kaataisi ajoa vaan poistaisi vääriä
    rivejä tai ei mitään, ja idempotenssi menisi rikki huomaamatta.
    """
    paiva = ds.replace("-", "")            # 2026-08-26 -> 20260826, ks. _date_part
    ehto = sql.SQL(
        "workcenter_id = %s AND source_id = %s AND ("
        " (event_type_id = %s AND {stop_date} = %s)"
        " OR (event_type_id <> %s AND {start_date} = %s)"
        ")"
    ).format(
        stop_date=sql.Identifier(target_column("stop_date")),
        start_date=sql.Identifier(target_column("start_date")),
    )
    workcenter = _workcenter_id(config)
    source = int(config["target"]["source_id"])
    return ehto, (workcenter, source,
                  EVENT_TYPE_PART_CHANGE, paiva, EVENT_TYPE_PART_CHANGE, paiva)


def _write_machine_events(config: dict, events: pd.DataFrame, ds: str) -> dict:
    """Kirjoittaa konetapahtumat kohdetauluun yhdessä transaktiossa.

    Vaiheet: tarkista taulu -> mahdollinen päivän poisto -> rivien lisäys.
    Kaikki yhdessä transaktiossa, joten epäonnistuessa taulu jää siihen tilaan jossa se
    oli ennen ajoa -- puolikasta päivää ei synny.

    write_mode valitsee tavan; replace_day poistaa päivän rivit ennen lisäystä
    _paivan_poistoehto-funktion rakentamalla ehdolla, joka tekee ajosta idempotentin.

    start_at/stop_at ovat aikavyöhykkeellisiä aikaleimoja. psycopg2 lähettää ne
    UTC-erotuksen kanssa, joten timestamptz-sarake tallentaa oikean hetken kannan
    TimeZone-asetuksesta riippumatta.

    Palauttaa lokitettavan yhteenvedon: tila, poistetut ja lisätyt rivit, kohdekanta ja
    tieto siitä oliko se varakanta.
    """
    mode = config["target"]["write_mode"]
    if mode not in VALID_WRITE_MODES:
        raise ValueError(f"Tuntematon write_mode '{mode}'. Sallitut: {sorted(VALID_WRITE_MODES)}")

    table = _target_table(config)
    # Sarakelista kohdetaulun nimillä; arvot poimitaan alla sisäisillä nimillä samassa
    # järjestyksessä, joten sarakkeet ja arvot vastaavat toisiaan.
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in TARGET_COLUMNS)

    # NaN/NaT -> None, jotta psycopg2 kirjoittaa NULLin eikä kaadu
    clean = events.astype(object).where(pd.notna(events), None)
    records = [tuple(row) for row in clean[MACHINE_EVENT_COLUMNS].itertuples(index=False)]

    insert = sql.SQL("INSERT INTO {table} ({columns}) VALUES %s").format(table=table, columns=columns)

    deleted = 0
    dsn, varakanta = _resolve_pg_dsn()
    kohde = _dsn_label(dsn)
    if varakanta:
        print(f"VAROITUS: PG_DSN puuttuu -- kirjoitetaan VARAKANTAAN {kohde}. "
              "Tämä ei välttämättä ole ulkoinen tietovarasto.")

    connection = psycopg2.connect(dsn)
    try:
        # "with connection" hoitaa commitin ja rollbackin; yhteys suljetaan erikseen
        # finallyssa, koska psycopg2:n context manager ei sulje sitä.
        with connection, connection.cursor() as cursor:
            _verify_target_table(cursor, config, kohde)

            if mode == "replace_day":
                ehto, arvot = _paivan_poistoehto(config, ds)
                cursor.execute(
                    sql.SQL("DELETE FROM {table} WHERE ").format(table=table) + ehto, arvot)
                deleted = cursor.rowcount

            # execute_values niputtaa rivit harvoiksi INSERTeiksi yksi kerrallaan
            # lähettämisen sijaan.
            if records:
                execute_values(cursor, insert.as_string(cursor), records, page_size=500)
    finally:
        connection.close()

    # Kohde lokitetaan ilman tunnuksia -- ks. _dsn_label.
    return {"write_mode": mode, "deleted": deleted, "inserted": len(records),
            "kohde": kohde, "varakanta": varakanta}
