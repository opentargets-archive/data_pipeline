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
        self.indication_with_preconcatenated_url = {"efo_id": "EFO_0003843",
            "efo_label": "Pain",
            "efo_uri": "...",
            "max_phase_for_indication": 1,
            "references": [{
                "source": "ClinicalTrials",
                "ids": [
                    "NCT01983969",
                    "NCT02589145",
                    "NCT02701673",
                    "NCT02961816"
                ],
                "urls": [
                    "https://clinicaltrials.gov/search?id=%22NCT01983969%22OR%22NCT02589145%22OR%22NCT02701673%22OR%22NCT02961816%22"
                ]
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

    def test_drug_with_no_indications_has_empty_aggregation(self):
        #given
        drug_with_no_inds = {}
        # when
        results = self.dp.generateAggregatedIndicationRefs(drug_with_no_inds)
        # then
        self.assertTrue(len(results) is 0)

    def test_drug_with_two_indications_has_two_refs(self):
        #given
        drug_with_two_inds = {}
        drug_with_two_inds["indications"] = [self.ind1, self.ind2]
        # when
        results = self.dp.generateAggregatedIndicationRefs(drug_with_two_inds)
        # then
        self.assertTrue(len(results) is 2)

    def test_drug_with_one_indications_with_two_refs_has_two_refs(self):
        # given
        drug_with_two_inds = {}
        drug_with_two_inds["indications"] = [self.dp.concatenate_two_indicators_with_matching_efos(self.ind1, self.ind2)]
        # when
        results = self.dp.generateAggregatedIndicationRefs(drug_with_two_inds)
        # then
        self.assertTrue(len(results) is 2)

    def test_drug_with_preconcatenated_url_decomposes_into_refs(self):
        # given
        drug = {}
        drug["indications"] = [self.indication_with_preconcatenated_url]
        # when
        results = self.dp.generateAggregatedIndicationRefs(drug)
        # then
        self.assertTrue(len(results) is 4)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDrugModule('test_indications_concatentate_when_1_source'))
    suite.addTest(TestDrugModule('test_indications_concatentate_when_n_sources'))
    suite.addTest(TestDrugModule('test_drug_with_no_indications_has_empty_aggregation'))
    suite.addTest(TestDrugModule('test_drug_with_two_indications_has_two_refs'))
    suite.addTest(TestDrugModule('test_drug_with_one_indications_with_two_refs_has_two_refs'))
    suite.addTest(TestDrugModule('test_drug_with_preconcatenated_url_decomposes_into_refs'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite())
