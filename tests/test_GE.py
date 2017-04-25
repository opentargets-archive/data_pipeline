import unittest
from mrtarget.modules import GE
from mrtarget.Settings import Config

ge_object = GE.GE()


class MyTestCase(unittest.TestCase):
    def test_request_to_panel_app(self):
        self.assertIn("Multiple endocrine tumours", "553f94b4bb5a1616e5ed4595", ge_object.request_to_panel_app())
        self.assertIn("Congenital myopathy", "553f94b6bb5a1616e5ed459a", ge_object.request_to_panel_app())
        self.assertIn("Renal tubular acidosis", "553f94d5bb5a1616e5ed45a4", ge_object.request_to_panel_app())

    def test_execute_ge_request(self):
        self.assertIn('Meningioma',ge_object.execute_ge_request())
        self.assertIn('ischemic stroke', ge_object.execute_ge_request())
        self.assertIn('Perlman Syndrome', ge_object.execute_ge_request())

    def test_request_to_zooma(self):
        self.assertIn('Meningioma', ge_object.request_to_zooma('Meningioma'))
        self.assertIn('http://www.orpha.net/ORDO/Orphanet_2495', ge_object.request_to_zooma('Meningioma')['Meningioma']['uri'])

    def test_evidence_strings(self):
        with open(Config.GE_EVIDENCE_STRING, 'r') as evidence_file:
            for row in evidence_file:
                self.assertIn("validated_against_schema_version", row)


if __name__ == '__main__':
    unittest.main()






