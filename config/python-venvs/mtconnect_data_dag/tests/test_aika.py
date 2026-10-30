"""
test_aika.py -- kellovirheen ja aikavyöhykkeen käsittely.

Kohdennetut testit: vuorokauden rajat, hakuikkuna ja kellonsiirtopäivät. Ei verkkoa
eikä tietokantaa. Ajetaan venvin tulkilla kontissa:

    docker exec <worker> /opt/airflow/config/python-venvs/mtconnect_data_dag/venv/bin/python \
        -m unittest discover -s /opt/airflow/config/python-venvs/mtconnect_data_dag/tests -v
"""

import copy
import sys
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mtconnect_data_core import vaiheet  # noqa: E402
from mtconnect_data_core.apurit import (  # noqa: E402
    _from_influx,
    _partition_ds,
    _resolve_interval,
    _to_influx,
    _to_local,
)
from mtconnect_data_core.konetapahtumat import _build_machine_events  # noqa: E402
from mtconnect_data_core.raakataso import _build_events, _classify_states  # noqa: E402
from mtconnect_data_core.vakiot import (  # noqa: E402
    DEFAULT_CONFIG,
    EVENT_TYPE_DAY,
    EVENT_TYPE_POWER_OFF,
    JOB_MAIN,
    SIGNAL_COLUMN,
)

KESAPAIVA = "2026-10-06"
TALVIAIKAAN = "2026-10-25"      # 25 h: klo 04 -> 03
KESAAIKAAN = "2027-03-28"       # 23 h: klo 03 -> 04
TUNTI = pd.Timedelta(hours=1)


def _config(**time) -> dict:
    """Oletusasetukset TUOVA-ympäristön kellovirheellä (6 h) ja yhdellä koneella.
    time-osion arvoja voi vaihtaa."""
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["time"].update(clock_error_hours=6, **time)
    config["machines"] = [{"workcenter_id": "1"}]
    return config


def _ctx(target_date: str) -> dict:
    return {"run_id": "testi", "target_date": target_date}


def _utc(text: str) -> pd.Timestamp:
    return pd.Timestamp(text, tz="UTC")


class VuorokaudenRajat(unittest.TestCase):
    """Ajopäivä on paikallinen kalenteripäivä, ei UTC-päivä eikä alku + 24 h."""

    def _tunnit(self, paiva: str) -> float:
        start, end = _resolve_interval(_config(), _ctx(paiva))
        return (end - start) / TUNTI

    def test_kesapaiva_alkaa_paikallisesta_keskiyosta(self):
        start, end = _resolve_interval(_config(), _ctx(KESAPAIVA))
        self.assertEqual(start, _utc("2026-10-05 21:00"))
        self.assertEqual(end, _utc("2026-10-06 21:00"))

    def test_talvipaiva_alkaa_paikallisesta_keskiyosta(self):
        start, end = _resolve_interval(_config(), _ctx("2026-12-01"))
        self.assertEqual(start, _utc("2026-11-30 22:00"))
        self.assertEqual(end, _utc("2026-12-01 22:00"))

    def test_kellonsiirtopaivien_pituus(self):
        self.assertEqual(self._tunnit(KESAPAIVA), 24)
        self.assertEqual(self._tunnit(TALVIAIKAAN), 25)
        self.assertEqual(self._tunnit(KESAAIKAAN), 23)

    def test_perakkaiset_paivat_eivat_mene_paallekkain(self):
        _, edellisen_loppu = _resolve_interval(_config(), _ctx("2026-10-24"))
        start, end = _resolve_interval(_config(), _ctx(TALVIAIKAAN))
        seuraavan_alku, _ = _resolve_interval(_config(), _ctx("2026-10-26"))
        self.assertEqual(edellisen_loppu, start)
        self.assertEqual(end, seuraavan_alku)

    def test_ds_on_paikallinen_paiva(self):
        for paiva in (KESAPAIVA, TALVIAIKAAN, KESAAIKAAN):
            self.assertEqual(_partition_ds(_config(), _ctx(paiva)), paiva)


class KellovirheJaPaikallinenAika(unittest.TestCase):
    def test_kesalla_korjaus_on_yhteensa_9_h(self):
        local = _to_local(_from_influx(_utc("2026-07-01 03:00"), _config()), _config())
        self.assertEqual(local.strftime("%Y-%m-%d %H:%M %z"), "2026-07-01 12:00 +0300")

    def test_talvella_korjaus_on_yhteensa_8_h(self):
        local = _to_local(_from_influx(_utc("2026-12-01 03:00"), _config()), _config())
        self.assertEqual(local.strftime("%Y-%m-%d %H:%M %z"), "2026-12-01 11:00 +0200")

    def test_muunnos_edestakaisin(self):
        stamp = _utc("2026-10-25 00:30")
        self.assertEqual(_to_influx(_from_influx(stamp, _config()), _config()), stamp)

    def test_sarja_ja_puuttuva_arvo(self):
        series = pd.Series([_utc("2026-07-01 03:00"), pd.NaT])
        local = _to_local(_from_influx(series, _config()), _config())
        self.assertEqual(local.iloc[0].hour, 12)
        self.assertTrue(pd.isna(local.iloc[1]))
        self.assertIsNone(_to_local(None, _config()))

    def test_toistuva_tunti_pysyy_jarjestyksessa(self):
        # 25.10.2026 paikallinen 03:30 esiintyy kahdesti: 00:30 UTC ja 01:30 UTC.
        eka = _to_local(_utc("2026-10-25 00:30"), _config())
        toka = _to_local(_utc("2026-10-25 01:30"), _config())
        self.assertEqual((eka.strftime("%H%M"), toka.strftime("%H%M")), ("0330", "0330"))
        self.assertLess(eka, toka)
        self.assertEqual(toka - eka, TUNTI)


class Hakuikkuna(unittest.TestCase):
    def _paahaut(self, paiva: str) -> dict:
        jobs = vaiheet.plan_influx_queries_for_interval(_config(), _ctx(paiva))
        return next(job for job in jobs if job["kind"] == JOB_MAIN)

    def test_haku_tehdaan_influxin_aikajanalla(self):
        # Paikallinen vrk 21:00 -> 21:00 UTC, kellovirhe -6 h, lookback/lookahead 12 h.
        job = self._paahaut(KESAPAIVA)
        self.assertEqual(job["start"], "2026-10-05T03:00:00Z")
        self.assertEqual(job["stop"], "2026-10-07T03:00:00Z")

    def test_kellonsiirtopaivan_haku_on_tunnin_pidempi(self):
        job = self._paahaut(TALVIAIKAAN)
        self.assertEqual(job["start"], "2026-10-24T03:00:00Z")
        self.assertEqual(job["stop"], "2026-10-26T04:00:00Z")


class Kellonsiirtopaiva(unittest.TestCase):
    """Koko laskenta hiljaiselle koneelle: yksi sammutusjakso koko vuorokauden."""

    def _tapahtumat(self, paiva: str) -> pd.DataFrame:
        config = _config()
        start, end = _resolve_interval(config, _ctx(paiva))
        tyhja = pd.DataFrame({"time": pd.Series(dtype="datetime64[ns, UTC]"),
                              SIGNAL_COLUMN: pd.Series(dtype=str), "value": pd.Series(dtype=float)})
        segments = _classify_states(tyhja, start, end, aggr="1min",
                                    threshold=5.0, override_threshold=50.0)
        raw = _build_events(segments, pd.DataFrame())
        return _build_machine_events(raw, start, end, config)

    def _rivi(self, events: pd.DataFrame, tyyppi: int) -> pd.Series:
        return events[events["event_type_id"] == tyyppi].iloc[0]

    def test_25_tunnin_vuorokausi(self):
        events = self._tapahtumat(TALVIAIKAAN)
        for tyyppi in (EVENT_TYPE_DAY, EVENT_TYPE_POWER_OFF):
            rivi = self._rivi(events, tyyppi)
            self.assertEqual(rivi["duration_min"], 1500)
            self.assertEqual((rivi["start_date"], rivi["start_time"]), ("20261025", "000000"))
            self.assertEqual((rivi["stop_date"], rivi["stop_time"]), ("20261026", "000000"))
        self.assertEqual(self._rivi(events, EVENT_TYPE_DAY)["power_off_pct"], 1.0)

    def test_23_tunnin_vuorokausi(self):
        rivi = self._rivi(self._tapahtumat(KESAAIKAAN), EVENT_TYPE_DAY)
        self.assertEqual(rivi["duration_min"], 1380)
        self.assertEqual(rivi["start_date"], "20270328")

    def test_start_at_on_oikea_hetki(self):
        rivi = self._rivi(self._tapahtumat(KESAPAIVA), EVENT_TYPE_DAY)
        self.assertEqual(rivi["start_at"], _utc("2026-10-05 21:00"))
        self.assertEqual(rivi["start_at"].isoformat(), "2026-10-06T00:00:00+03:00")
        self.assertEqual(rivi["duration_min"], 1440)

    def test_ei_poikkeamalokia_kellonsiirtopaivana(self):
        with mock.patch("mtconnect_data_core.konetapahtumat._log") as loki:
            self._tapahtumat(TALVIAIKAAN)
        self.assertNotIn("machine_day_window_poikkeaa",
                         [kutsu.args[0] for kutsu in loki.call_args_list])


if __name__ == "__main__":
    unittest.main()
