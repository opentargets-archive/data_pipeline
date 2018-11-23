from mrtarget.common.connection import PipelineConnectors

def test_answer():
    c = PipelineConnectors()
    c.init_services_connections("localhost",35000, False)

    c2 = PipelineConnectors()
    c2.init_services_connections("localhost",35000, False)
    assert c.r_instance == c2.r_instance
    assert c.r_server != c2.r_server