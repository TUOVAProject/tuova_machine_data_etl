"""
test_regressio.py -- koko laskenta synteettisellä päivällä, tulos tallennettua vasten.

Turvaverkko muutoksille: synteettinen data (synteettinen_data.py) kulkee samat vaiheet
kuin DAGissa -- haun suunnittelu, yhdistys, tilajaksot, kappaleet, raakataso ja
konetapahtumat -- ja tulosta verrataan hakemiston odotetut/ tiedostoihin:

    raakataso_<päivä>.csv        debug-partitio sellaisenaan
    konetapahtumat_<päivä>.csv   kohdetauluun kirjoitettavat rivit (DAGin sisäiset nimet)

Influx korvataan emulaatiolla (_hae), joka poimii suunnitellun haun pisteet samoin
rajauksin ja aggregoinnein. Se ei ole tarkka Flux-kopio, mutta se on sama joka ajolla,
joten tuloksen muutos johtuu aina laskennasta tai haun suunnittelusta.

Kun muutos on tarkoituksellinen, odotetut tiedostot kirjoitetaan uudelleen ja muutos
tarkistetaan versionhallinnan diffistä:

    dev/test.sh paivita
"""

import contextlib
import copy
import difflib
import io
import os
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import synteettinen_data  # noqa: E402
from mtconnect_data_core import vaiheet  # noqa: E402
from mtconnect_data_core.vakiot import (  # noqa: E402
    DEFAULT_CONFIG,
    EVENT_TYPE_CUTTING,
    EVENT_TYPE_DAY,
    EVENT_TYPE_IDLE,
    EVENT_TYPE_PART_CHANGE,
    EVENT_TYPE_POWER_OFF,
    EVENT_TYPE_PRODUCTION,
)

ODOTETUT_DIR = Path(__file__).resolve().parent / "odotetut"
# dev/test.sh paivita asettaa tämän: odotetut tiedostot kirjoitetaan vertaamisen sijaan.
PAIVITA = os.environ.get("PAIVITA_ODOTETUT") == "1"

TAVALLINEN_PAIVA = "2026-10-06"
KELLONSIIRTOPAIVA = "2026-10-25"      # 25 h


def _asetukset(data_root: str) -> dict:
    """Oletusasetukset, joissa TUOVA-ympäristön arvot on kirjoitettu auki.

    Näin oletusarvon muutos ei muuta odotettua tulosta -- vain laskennan muutos muuttaa.
    """
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["time"].update(clock_error_hours=6, timezone="Europe/Helsinki")
    config["target"]["source_id"] = 2
    config["machines"] = [{"workcenter_id": "1"}]
    config["files"]["data_root"] = data_root
    return config


def _lahde(pisteet: list) -> pd.DataFrame:
    """Pisteet kuten Influx ne palauttaa: rivi per mittaus, tagit omina sarakkeinaan."""
    return pd.DataFrame([
        {"time": piste.at, "measurement": piste.measurement, "field": piste.field_key,
         "value": piste.value, **piste.tags}
        for piste in pisteet
    ])


def _hae(lahde: pd.DataFrame, job: dict, config: dict) -> pd.DataFrame:
    """Emuloi yhden suunnitellun Flux-haun: aikaväli, measurement, laite, nimet, aggregointi.

    aggregateWindow: ikkunat alkavat epochista, rivin aikaleimaksi tulee ikkunan loppu
    (enintään haun loppu) eikä tyhjiä ikkunoita luoda -- kuten Fluxissa oletuksena.
    """
    item_tag, device_tag = config["source"]["item_tag"], config["source"]["device_tag"]
    device = config["machines"][0].get("device")
    start, stop = pd.Timestamp(job["start"]), pd.Timestamp(job["stop"])
    rivit = lahde[(lahde["measurement"] == job["measurement"])
                  & lahde[item_tag].isin(job["fields"])
                  & (lahde["time"] >= start) & (lahde["time"] < stop)]
    if device:
        rivit = rivit[rivit[device_tag] == device]
    rivit = rivit.sort_values([item_tag, "time"], kind="mergesort")
    if not job.get("agg_fn"):
        return rivit.reset_index(drop=True)

    ikkuna = pd.Timedelta(job["window"])
    rivit = rivit.assign(time=(rivit["time"].dt.floor(ikkuna) + ikkuna).clip(upper=stop))
    muut = [column for column in rivit.columns if column not in (item_tag, "time", "value")]
    return (
        rivit.groupby([item_tag, "time"], sort=True)
        .agg({**{column: "first" for column in muut}, "value": job["agg_fn"]})
        .reset_index()
    )


class Regressio(unittest.TestCase):
    """Synteettinen päivä läpi koko laskennan. Ks. moduulin docstring."""

    def _aja(self, paiva: str) -> tuple[str, pd.DataFrame]:
        """Palauttaa raakatason CSV:n tekstinä ja konetapahtumat DataFramena."""
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            config = _asetukset(tmp)
            ctx = {"run_id": "regressio", "target_date": paiva}
            lahde = _lahde(synteettinen_data.generoi(config, date.fromisoformat(paiva)))

            fetched = []
            for job in vaiheet.plan_influx_queries_for_interval(config, ctx):
                frame = _hae(lahde, job, config)
                path = Path(tmp) / f"raw_{job['kind']}_{job['key']}.pkl"
                frame.to_pickle(path)
                fetched.append({"key": job["key"], "kind": job["kind"],
                                "path": str(path), "rows": len(frame)})

            combined = vaiheet.combine_and_validate_raw_data(config, ctx, fetched)
            raw = vaiheet.build_raw_event_rows(
                config, ctx,
                vaiheet.classify_machine_state_segments(config, ctx, combined),
                vaiheet.compute_part_cycle_events(config, ctx, combined),
            )
            partitio = vaiheet.write_daily_event_partition(config, ctx, raw)
            raakataso = Path(partitio["path"]).read_text(encoding="utf-8")
            events = pd.read_pickle(vaiheet.build_machine_events(config, ctx, raw))
        return raakataso, events

    def _vertaa(self, nimi: str, tulos: str) -> None:
        polku = ODOTETUT_DIR / nimi
        if PAIVITA:
            ODOTETUT_DIR.mkdir(exist_ok=True)
            polku.write_text(tulos, encoding="utf-8")
            return
        if not polku.exists():
            self.fail(f"Odotettu tiedosto puuttuu: {polku}. Luo se: dev/test.sh paivita")
        odotettu = polku.read_text(encoding="utf-8")
        if tulos != odotettu:
            diff = difflib.unified_diff(odotettu.splitlines(), tulos.splitlines(),
                                        "odotettu", "tulos", lineterm="", n=0)
            self.fail(f"{nimi} poikkeaa odotetusta:\n" + "\n".join(list(diff)[:40]))

    def _tarkista_paiva(self, paiva: str) -> pd.DataFrame:
        raakataso, events = self._aja(paiva)
        self._vertaa(f"raakataso_{paiva}.csv", raakataso)
        self._vertaa(f"konetapahtumat_{paiva}.csv", events.to_csv(index=False))
        return events

    def test_tavallinen_paiva(self):
        events = self._tarkista_paiva(TAVALLINEN_PAIVA)
        # Generaattorin päiväohjelmassa esiintyy jokainen tapahtumatyyppi.
        self.assertEqual(set(events["event_type_id"]), {
            EVENT_TYPE_PRODUCTION, EVENT_TYPE_PART_CHANGE, EVENT_TYPE_CUTTING,
            EVENT_TYPE_IDLE, EVENT_TYPE_POWER_OFF, EVENT_TYPE_DAY})

    def test_kellonsiirtopaiva(self):
        events = self._tarkista_paiva(KELLONSIIRTOPAIVA)
        paiva = events[events["event_type_id"] == EVENT_TYPE_DAY].iloc[0]
        self.assertEqual(paiva["duration_min"], 1500)


if __name__ == "__main__":
    unittest.main()
