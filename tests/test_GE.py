from modules import GE
from settings import Config

ge_object = GE.GE()


def test_request_to_panel_app():
    assert "Multiple endocrine tumours", "553f94b4bb5a1616e5ed4595" in ge_object.request_to_panel_app()
    assert "Congenital myopathy", "553f94b6bb5a1616e5ed459a" in ge_object.request_to_panel_app()
    assert "Renal tubular acidosis", "553f94d5bb5a1616e5ed45a4" in ge_object.request_to_panel_app()


def test_execute_ge_request():
    assert ge_object.panel_app_info is not None


def test_execute_zooma():
    assert ge_object.high_confidence_mappings is not None


def test_evidence_strings():
    assert ge_object.evidence_strings is not None


def test_write_evidence_strings():
    assert Config.GE_EVIDENCE_STRING is not None







