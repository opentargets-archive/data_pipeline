import unittest

from mrtarget.modules.Drug import DrugProcess


class TestDrugModule(unittest.TestCase):
    def setUp(self):
        self.dp = DrugProcess("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        self.ind1 = {"efo_id": "EFO_0003843",
            "efo_label": "Pain",
            "efo_uri": "...",
            "max_phase_for_indication": 1,
            "references": [{
                "source": "ClinicalTrials",
                "ids": ["NCT02229539"],
                "urls": ["https://clinicaltrials.gov/search?id=%22NCT02229539%22"]
            }]
            }
        self.ind2 = {"efo_id": "EFO_0003843",
            "efo_label": "Pain",
            "efo_uri": "...",
            "max_phase_for_indication": 4,
            "references": [{
                "source": "ClinicalTrials",
                "ids": ["NCT03396250"],
                "urls": ["https://clinicaltrials.gov/search?id=%22NCT03396250%22"]
            }]
            }
        self.ind3 = {"efo_id": "EFO_0003843",
            "efo_label": "Pain",
            "efo_uri": "...",
            "max_phase_for_indication": 4,
            "references": [{
                "source": "DailyMed",
                "ids": ["047e8b1b-8888-40b7-b593-434e92eb333c"],
                "urls": ["https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?137038a6-12ed-47fd-a97c-01073c5141d1"]
            }]
            }
        self.ind4 = {"efo_id": "EFO_0003843",
            "efo_label": "Pain",
            "efo_uri": "...",
            "max_phase_for_indication": 5,
            "references": [{
                "source": "DummySource",
                "ids": ["047e8b1b-8888-40b7-b593-434e92eb333c"],
                "urls": ["https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?137038a6-12ed-47fd-a97c-01073c5141d1"]
            }]
            }

    def test_indications_concatenate_when_1_source(self):
        self.dp.concatenate_two_indicators_with_matching_efos(self.ind1, self.ind2)
        self.assertTrue(self.ind1.get("max_phase_for_indication") == 4)
        self.assertTrue(len(self.ind1.get("references")[0]["ids"]) == 2)
        self.assertTrue(len(self.ind1.get("references")[0]["urls"]) == 2)

    def test_indications_concatenate_when_n_sources(self):
        # given two sources
        self.dp.concatenate_two_indicators_with_matching_efos(self.ind1, self.ind3)
        self.dp.concatenate_two_indicators_with_matching_efos(self.ind1, self.ind4)
        self.assertTrue(self.ind1.get("max_phase_for_indication") == 5)
        self.assertTrue(len(self.ind1.get("references")) == 3)
        self.assertTrue(len(self.ind1.get("references")[0]["ids"]) == 1)
        self.assertTrue(len(self.ind1.get("references")[0]["urls"]) == 1)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDrugModule('test_indications_concatentate_when_1_source'))
    suite.addTest(TestDrugModule('test_indications_concatentate_when_n_sources'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())
