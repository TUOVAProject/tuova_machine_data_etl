"""
test_asetukset.py -- asetustiedoston luku, oletukset ja tarkistukset.

Jokainen testi kirjoittaa pienen YAML-tiedoston ja lukee sen load_config-vaiheella.
Ei verkkoa eikä tietokantaa. Ajetaan venvin tulkilla kontissa (dev/test.sh).
"""

import contextlib
import copy
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mtconnect_data_core import asetukset, vaiheet  # noqa: E402
from mtconnect_data_core.vakiot import (  # noqa: E402
    CONFIG_FILE,
    DEFAULT_CONFIG,
    SIGNAL_PART_COUNT,
    SIGNAL_PROGRAM,
    SIGNAL_RUNNING_TIME,
)

# Pienin kelpaava asetustiedosto: kone on ainoa pakollinen tieto.
KONE = "machines:\n  - workcenter_id: 1\n"
EXAMPLE_FILE = CONFIG_FILE.with_name("mtconnect_data_config.example.yaml")


def _lataa(yaml_teksti: str) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "config.yaml"
        path.write_text(yaml_teksti, encoding="utf-8")
        with mock.patch.object(asetukset, "CONFIG_FILE", path), \
                contextlib.redirect_stdout(io.StringIO()):
            return vaiheet.load_config()


class Oletukset(unittest.TestCase):
    def test_oletukset(self):
        config = _lataa(KONE)
        self.assertEqual(config["time"]["clock_error_hours"], 0)
        self.assertEqual(config["time"]["timezone"], "Europe/Helsinki")
        self.assertEqual(config["signals"][SIGNAL_RUNNING_TIME],
                         {"measurement": "mtc_metrics", "name": "ptotalrunningtime", "unit": "s"})

    def test_oletukset_eivat_muutu_ajossa(self):
        alkuperainen = copy.deepcopy(DEFAULT_CONFIG)
        config = _lataa(KONE)
        config["signals"][SIGNAL_RUNNING_TIME]["name"] = "muutettu"
        self.assertEqual(DEFAULT_CONFIG, alkuperainen)

    def test_tuntematon_osio_kirjataan(self):
        with mock.patch.object(asetukset, "_log") as loki:
            _lataa(KONE + "extra:\n  a: 1\n")
        tapahtumat = {kutsu.args[0]: kutsu.kwargs for kutsu in loki.call_args_list}
        self.assertEqual(tapahtumat["asetus_tuntematon_avain"]["avaimet"], ["extra"])

    def test_esimerkkitiedosto_kelpaa(self):
        config = _lataa(EXAMPLE_FILE.read_text(encoding="utf-8"))
        self.assertEqual(config["time"]["clock_error_hours"], 0)

    def test_asennettu_asetustiedosto_kelpaa(self):
        with contextlib.redirect_stdout(io.StringIO()):
            config = vaiheet.load_config()
        self.assertEqual(len(config["machines"]), 1)


class Koneet(unittest.TestCase):
    def test_kone_on_pakollinen(self):
        with self.assertRaisesRegex(ValueError, "machines ei ole yhtään konetta"):
            _lataa("time: {}\n")

    def test_workcenter_id_on_pakollinen(self):
        with self.assertRaisesRegex(ValueError, r"machines\[0\]: puuttuu workcenter_id"):
            _lataa("machines:\n  - device: SIM-0001\n")

    def test_useampi_kone_ei_viela_tuettu(self):
        with self.assertRaisesRegex(ValueError, "tukee toistaiseksi 1"):
            _lataa("machines:\n  - workcenter_id: 1\n  - workcenter_id: 2\n")

    def test_tuntematon_avain_kaataa(self):
        with self.assertRaisesRegex(ValueError, "tuntemattomat avaimet"):
            _lataa("machines:\n  - workcenter_id: 1\n    devise: SIM-0001\n")


class VanhaMuoto(unittest.TestCase):
    def test_vanha_tiedosto_kaatuu_ohjeella(self):
        with self.assertRaisesRegex(ValueError, "aikaikkuna -> time.*Upgrading"):
            _lataa("aikaikkuna:\n  clock_error_hours: 6\nkohde:\n  workcenter_id: '1'\n")

    def test_poistettu_avain_kaataa(self):
        with self.assertRaisesRegex(ValueError, "clock_error_hours"):
            _lataa(KONE + "time:\n  time_offset_hours: 9\n")

    def test_tuntematon_aikavyohyke_kaataa(self):
        with self.assertRaisesRegex(ValueError, "Tuntematon aikavyöhyke"):
            _lataa(KONE + "time:\n  timezone: Europe/Helsinky\n")


class Signaalit(unittest.TestCase):
    def test_nimen_voi_vaihtaa(self):
        config = _lataa(KONE + "signals:\n  program: {measurement: status, name: Program}\n")
        self.assertEqual(config["signals"][SIGNAL_PROGRAM], {"measurement": "status", "name": "Program"})
        # Muut signaalit pysyvät oletuksina.
        self.assertEqual(config["signals"][SIGNAL_PART_COUNT]["name"], "ppartcount")

    def test_laskurin_oletusyksikko(self):
        config = _lataa(KONE + "signals:\n  running_time: {measurement: m, name: run}\n")
        self.assertEqual(config["signals"][SIGNAL_RUNNING_TIME]["unit"], "s")

    def test_tuntematon_yksikko_kaataa(self):
        with self.assertRaisesRegex(ValueError, "tuntematon yksikkö 'sec'"):
            _lataa(KONE + "signals:\n  running_time: {measurement: m, name: run, unit: sec}\n")

    def test_yksikko_vain_laskureille(self):
        with self.assertRaisesRegex(ValueError, r"signals.program: tuntemattomat avaimet \['unit'\]"):
            _lataa(KONE + "signals:\n  program: {measurement: m, name: p, unit: s}\n")

    def test_puuttuva_nimi_kaataa(self):
        with self.assertRaisesRegex(ValueError, "signals.program: puuttuu name"):
            _lataa(KONE + "signals:\n  program: {measurement: m}\n")


if __name__ == "__main__":
    unittest.main()
