from modules import GE
from settings import Config

ge_object = GE.GE()


def test_request_to_panel_app():
    assert "Multiple endocrine tumours", "553f94b4bb5a1616e5ed4595" in ge_object.request_to_panel_app()
    assert "Congenital myopathy", "553f94b6bb5a1616e5ed459a" in ge_object.request_to_panel_app()
    assert "Renal tubular acidosis", "553f94d5bb5a1616e5ed45a4" in ge_object.request_to_panel_app()


def test_execute_ge_request():
    assert 'Meningioma' in ge_object.execute_ge_request()
    assert 'ischemic stroke' in ge_object.execute_ge_request()
    assert 'Perlman Syndrome' in ge_object.execute_ge_request()


def test_request_to_zooma():
    assert 'Meningioma' in ge_object.request_to_zooma('Meningioma')
    assert 'http://www.orpha.net/ORDO/Orphanet_2495' in ge_object.request_to_zooma('Meningioma')['Meningioma']['uri']


def test_evidence_strings():
    with open(Config.GE_EVIDENCE_STRING, 'r') as evidence_file:
        for row in evidence_file:
            assert "validated_against_schema_version" in row







