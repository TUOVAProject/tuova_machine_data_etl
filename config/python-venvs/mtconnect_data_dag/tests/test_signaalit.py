"""
test_signaalit.py -- hakujen johtaminen signaaleista, Flux-kysely ja käännös signaaleiksi.

Ei verkkoa eikä tietokantaa. Ajetaan venvin tulkilla kontissa (dev/test.sh).
"""

import contextlib
import copy
import io
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mtconnect_data_core import influx, vaiheet  # noqa: E402
from mtconnect_data_core.vakiot import (  # noqa: E402
    DEFAULT_CONFIG,
    JOB_MAIN,
    JOB_WARMUP,
    SIGNAL_COLUMN,
    SIGNAL_PROGRAM,
    SIGNAL_RUNNING_TIME,
    SIGNAL_SPINDLE_OVERRIDE,
)


def _config() -> dict:
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["machines"] = [{"workcenter_id": "1"}]
    return config


class Haut(unittest.TestCase):
    def test_haut_johdetaan_signaaleista(self):
        queries = {query["key"]: query for query in influx._signal_queries(_config())}
        self.assertEqual(list(queries), [
            "counter_mtc_metrics", "raw_mtc_metrics", "raw_mtc_status",
            "warmup_mtc_metrics", "warmup_mtc_status"])
        laskurit = queries["counter_mtc_metrics"]
        self.assertEqual(sorted(laskurit["fields"]), [
            "ptotalcuttingtime", "ptotaloperatingtime", "ptotalrunningtime", "ptotalspindleruntime"])
        self.assertEqual((laskurit["window"], laskurit["agg_fn"]), ("1m", "max"))

    def test_lammittely_vain_tilamaisille(self):
        queries = {query["key"]: query for query in influx._signal_queries(_config())}
        self.assertEqual(queries["warmup_mtc_metrics"]["fields"], ["pfovr", "s1ovr"])
        self.assertEqual(queries["warmup_mtc_status"]["fields"], ["pprogram"])

    def test_nimi_vaihtuu_hakuun(self):
        config = _config()
        config["signals"][SIGNAL_PROGRAM] = {"measurement": "status", "name": "Program"}
        keys = [query["key"] for query in influx._signal_queries(config)]
        self.assertIn("raw_status", keys)
        self.assertNotIn("raw_mtc_status", keys)

    def test_lammittely_paattyy_haun_alkuun(self):
        with contextlib.redirect_stdout(io.StringIO()):
            jobs = vaiheet.plan_influx_queries_for_interval(
                _config(), {"run_id": "testi", "target_date": "2026-10-06"})
        main = next(job for job in jobs if job["kind"] == JOB_MAIN)
        warmup = next(job for job in jobs if job["kind"] == JOB_WARMUP)
        self.assertEqual(warmup["stop"], main["start"])
        self.assertEqual((warmup["window"], warmup["agg_fn"]), ("7d", "last"))


class FluxKysely(unittest.TestCase):
    def _kysely(self, **kwargs) -> str:
        args = dict(bucket="b", start="s", stop="e", measurement="m", fields=["x"],
                    window=None, agg_fn=None, item_tag="name", device_tag="device")
        return influx._build_query(**{**args, **kwargs})

    def test_laiterajaus(self):
        self.assertIn('r["device"] == "SIM-1"', self._kysely(device="SIM-1"))

    def test_ilman_laitetta_ei_rajausta(self):
        self.assertNotIn('r["device"]', self._kysely())

    def test_nimitagi_asetuksista(self):
        self.assertIn('r["item"] == "x"', self._kysely(item_tag="item"))

    def test_lainausmerkki_escapoidaan(self):
        self.assertIn('r["name"] == "a\\"b"', self._kysely(fields=['a"b']))


class Signaaleiksi(unittest.TestCase):
    def _data(self) -> pd.DataFrame:
        return pd.DataFrame({
            "time": pd.to_datetime(["2026-10-06 06:00"] * 3, utc=True),
            "measurement": ["mtc_metrics", "mtc_status", "mtc_metrics"],
            "name": ["ptotalrunningtime", "pprogram", "tuntematon"],
            "value": [2.0, "P1", 1.0],
        })

    def test_lahteen_nimet_signaaleiksi(self):
        tulos = influx._to_signals(_config(), self._data())
        self.assertEqual(list(tulos[SIGNAL_COLUMN]), [SIGNAL_RUNNING_TIME, SIGNAL_PROGRAM])

    def test_laskurin_yksikko_sekunneiksi(self):
        config = _config()
        config["signals"][SIGNAL_RUNNING_TIME]["unit"] = "min"
        tulos = influx._to_signals(config, self._data())
        self.assertEqual(tulos.loc[tulos[SIGNAL_COLUMN] == SIGNAL_RUNNING_TIME, "value"].iloc[0], 120.0)
        self.assertEqual(tulos.loc[tulos[SIGNAL_COLUMN] == SIGNAL_PROGRAM, "value"].iloc[0], "P1")

    def test_puuttuva_nimitagi_kaataa(self):
        config = _config()
        config["source"]["item_tag"] = "item"
        with self.assertRaisesRegex(ValueError, "puuttuu tagi 'item'"):
            influx._to_signals(config, self._data())

    def test_puuttuvat_pakolliset_nimetaan(self):
        tulos = influx._to_signals(_config(), self._data())
        puuttuvat = influx._missing_signals(_config(), tulos)
        self.assertIn("part_count (mtc_metrics/ppartcount)", puuttuvat)
        self.assertFalse(any(signal.startswith(SIGNAL_RUNNING_TIME) for signal in puuttuvat))
        # spindle_override ei ole pakollinen
        self.assertFalse(any(signal.startswith(SIGNAL_SPINDLE_OVERRIDE) for signal in puuttuvat))


if __name__ == "__main__":
    unittest.main()
