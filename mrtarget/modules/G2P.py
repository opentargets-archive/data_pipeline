import opentargets.model.core as opentargets
import opentargets.model.bioentity as bioentity
import opentargets.model.evidence.phenotype as evidence_phenotype
import opentargets.model.evidence.core as evidence_core
import opentargets.model.evidence.linkout as evidence_linkout
import opentargets.model.evidence.association_score as association_score
import opentargets.model.evidence.mutation as evidence_mutation
import json
import sys
import logging

'''
Extracted input JSONs from Postgres with the following command
psql -h localhost -d cttv_core_test -p 5432 -U tvdev -A -F $'\t' -X -t -c "SELECT JSON_BUILD_OBJECT('disease', disease, 'target', target, 'disease_label', disease_label, 'gene_symbol', gene_symbol) FROM gene2phenotype.final_data_for_evstrs_jul2016" >final_data_for_evstrs_jul2016.json
This is a skeleton only, need to add fields for score and evidence codes.
'''

class G2P():

    def __init__(self):
        self.evidence_strings = list()
        self.logger = logging.getLogger(__name__)

    def read_file(self, source_file):


        for doc_json in open(source_file, 'r'):
            doc_ds = json.loads(doc_json)
            (target, disease, name, symbol) = (doc_ds['target'],
                                               doc_ds['disease'],
                                               doc_ds['disease_label'],
                                               doc_ds['gene_symbol'])
            obj = opentargets.Literature_Curated(type='genetic_literature')
            provenance_type = evidence_core.BaseProvenance_Type(
                database=evidence_core.BaseDatabase(
                    id="Gene2Phenotype",
                    version='v0.1',
                    dbxref=evidence_core.BaseDbxref(
                        url="http://www.ebi.ac.uk/gene2phenotype",
                        id="Gene2Phenotype", version="v0.1")),
                literature=evidence_core.BaseLiterature(
                    references=[evidence_core.Single_Lit_Reference(lit_id="http://europepmc.org/abstract/MED/25529582")]
                )
            )
            obj.access_level = "public"
            obj.sourceID = "gene2phenotype"
            obj.validated_against_schema_version = "1.2.3"
            obj.unique_association_fields = {"target": target, "disease_uri": disease, "source_id": "gene2phenotype"}
            obj.target = bioentity.Target(id=[target],
                                          activity="http://identifiers.org/cttv.activity/unknown",
                                          target_type='http://identifiers.org/cttv.target/gene_evidence',
                                          target_name=symbol)
            # http://www.ontobee.org/ontology/ECO?iri=http://purl.obolibrary.org/obo/ECO_0000204 -- An evidence type that is based on an assertion by the author of a paper, which is read by a curator.
            resource_score = association_score.Probability(
                type="probability",
                method=association_score.Method(
                    description="NA",
                    reference="NA",
                    url="NA"),
                value=1)

            obj.disease = bioentity.Disease(id=[disease], name=[name])
            obj.evidence = evidence_core.Literature_Curated()
            obj.evidence.is_associated = True
            obj.evidence.evidence_codes = ["http://purl.obolibrary.org/obo/ECO_0000204"]
            obj.evidence.provenance_type = provenance_type
            obj.evidence.date_asserted = '2016-07-29'
            obj.evidence.provenance_type = provenance_type
            obj.evidence.resource_score = resource_score
            linkout = evidence_linkout.Linkout(
                url='http://www.ebi.ac.uk/gene2phenotype/gene2phenotype-webcode/cgi-bin/handler.cgi?panel=ALL&search_term=%s' % (
                symbol,),
                nice_name='Gene2Phenotype%s' % (symbol))
            obj.evidence.urls = [linkout]
            error = obj.validate(logging)
            if error > 0:
                self.logger.error(obj.to_JSON())
                sys.exit(1)
            else:
                self.evidence_strings.append(obj)


    def write_evidence_strings(self, filename):
        self.logger.info("Writing IntOGen evidence strings")
        with open(filename, 'w') as tp_file:
            n = 0
            for evidence_string in self.evidence_strings:
                n += 1
                self.logger.info(evidence_string.disease.id[0])
                # get max_phase_for_all_diseases
                error = evidence_string.validate(logging)
                if error == 0:
                    tp_file.write(evidence_string.to_JSON(indentation=None) + "\n")
                else:
                    self.logger.error("REPORTING ERROR %i" % n)
                    self.logger.error(evidence_string.to_JSON(indentation=4))
                    # sys.exit(1)
        tp_file.close()

def main():
    source_file = sys.argv[1]
    g2p = G2P()
    g2p.read_file(source_file)
    g2p.write_evidence_strings('/Users/koscieln/Documents/data/ftp/cttv001/upload/submissions/cttv001_gene2phenotype-29-07-2016.json')

if __name__ == "__main__":
    main()