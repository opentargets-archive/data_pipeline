from modules import GE
from settings import Config


def test_request_to_panel_app():
    assert "Multiple endocrine tumours", "553f94b4bb5a1616e5ed4595" in GE.request_to_panel_app()
    assert "Congenital myopathy", "553f94b6bb5a1616e5ed459a" in GE.request_to_panel_app()
    assert "Renal tubular acidosis", "553f94d5bb5a1616e5ed45a4" in GE.request_to_panel_app()


def test_check_cache():
    assert GE.check_cache(filename="/tmp/553f94b4bb5a1616e5ed4595.json") is not None
    assert GE.check_cache(filename="/tmp/553f94b6bb5a1616e5ed459a.json") is not None
    assert GE.check_cache(filename="/tmp/553f94d5bb5a1616e5ed45a4.json") is not None


def test_update_cache():
    assert GE.update_cache(data=None) is None


def test_execute_ge_request():
    assert Config.PANEL_APP_INFO is not None


def test_execute_zomma():
    assert Config.ZOOMA_DISEASE_MAPPING is not None

GE_object = GE.GE()


def test_disease_mapping():
    assert GE_object.disease_mappings is not None


def test_evidence_strings():
    assert GE_object.evidence_strings is not None


def test_process_disease_mapping_file():
    assert GE_object.process_disease_mapping_file() is not None


def test_write_evidence_strings():
    assert Config.GE_EVIDENCE_STRING is not None







