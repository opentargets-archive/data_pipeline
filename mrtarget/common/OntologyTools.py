import sys
reload(sys)
sys.setdefaultencoding("utf8")
import ConfigParser
from mrtarget.Settings import file_or_resource

ONTOLOGY_CONFIG = ConfigParser.ConfigParser()
ONTOLOGY_CONFIG.read(file_or_resource('ontology_config.ini'))

class OntologyLookup(object):

    def __init__(self):
        pass

    def get_transitive_closure(self, database_name, prefix='', from_clause='', node_type='rdfs:subClassOf', node_filter=None):
        sparql_query = ONTOLOGY_CONFIG.get('sparql_templates', 'transitive_closure')

        print sparql_query%(prefix, from_clause, node_type, node_filter)


if __name__ == '__main__':
    lookup = OntologyLookup()
    lookup.get_transitive_closure('ECO', prefix='PREFIX obo: <http://purl.obolibrary.org/obo/>\n', node_filter='<http://www.targetvalidation.org/evidence/cttv_evidence>')
    lookup.get_transitive_closure('CHEMBL', node_filter='<http://purl.obolibrary.org/obo/CHEMBL_PC_0>')
    lookup.get_transitive_closure(
        'EFO',
        from_clause=' FROM <http://www.targetvalidation.org/>\nFROM <http://www.ebi.ac.uk/efo/>\nFROM <http://www.ebi.ac.uk/efo/rare_albuminuria/>\n',
             node_filter='<http://www.targetvalidation.org/cttv_root>')



