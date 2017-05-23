import logging
import logging.config
import time
from mrtarget.Settings import file_or_resource

logging.config.fileConfig(file_or_resource('logging.ini'),
                              disable_existing_loggers=False)
from intermine.webservice import Service


PHENOTYPE_CATEGORIES = {
    "MP:0005386" : "behavior/neurological phenotype",
    "MP:0005375" : "adipose tissue phenotype",
    "MP:0005385" : "cardiovascular system phenotype",
    "MP:0005384" : "cellular phenotype",
    "MP:0005382" : "craniofacial phenotype",
    "MP:0005381" : "digestive/alimentary phenotype",
    "MP:0005380" : "embryo phenotype",
    "MP:0005379" : "endocrine/exocrine phenotype",
    "MP:0005378" : "growth/size/body region phenotype",
    "MP:0005377" : "hearing/vestibular/ear phenotype",
    "MP:0005397" : "hematopoietic system phenotype",
    "MP:0005376" : "homeostasis/metabolism phenotype",
    "MP:0005387" : "immune system phenotype",
    "MP:0010771" : "integument phenotype",
    "MP:0005371" : "limbs/digits/tail phenotype",
    "MP:0005370" : "liver/biliary system phenotype",
    "MP:0010768" : "mortality/aging",
    "MP:0005369" : "muscle phenotype",
    "MP:0002006" : "neoplasm",
    "MP:0003631" : "nervous system phenotype",
    "MP:0002873" : "normal phenotype",
    "MP:0001186" : "pigmentation phenotype",
    "MP:0005367" : "renal/urinary system phenotype",
    "MP:0005389" : "reproductive system phenotype",
    "MP:0005388" : "respiratory system phenotype",
    "MP:0005390" : "skeleton phenotype",
    "MP:0005394" : "taste/olfaction phenotype",
    "MP:0005391" : "vision/eye phenotype"
}

class MouseminePhenotypeETL(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.service = Service("http://www.mousemine.org/mousemine/service")
        self.genes = dict()

    def get_genotype_phenotype(self):

        for (id, category) in PHENOTYPE_CATEGORIES.items():

            query = self.service.new_query("OntologyAnnotation")
            query.add_constraint("evidence.baseAnnotations.subject", "Genotype")
            query.add_constraint("subject", "SequenceFeature")
            query.add_constraint("ontologyTerm", "MPTerm")
            query.add_constraint("ontologyTerm.parents", "MPTerm")
            query.add_view(
                "subject.primaryIdentifier", "subject.symbol",
                "evidence.baseAnnotations.subject.symbol",
                "evidence.baseAnnotations.subject.background.name",
                "evidence.baseAnnotations.subject.zygosity", "ontologyTerm.identifier",
                "ontologyTerm.name"
            )
            query.add_sort_order("OntologyAnnotation.subject.symbol", "ASC")
        #query.add_constraint("ontologyTerm.parents", "LOOKUP", "*circulating glucose*", code="A")

            query.add_constraint("ontologyTerm.parents", "LOOKUP", "*%s*"%category, code="A")

            for row in query.rows():
                gene_id = row["subject.primaryIdentifier"]
                gene_symbol = row["subject.symbol"]
                if gene_id not in self.genes:
                    self.genes[gene_id] = dict()
                if id not in self.genes[gene_id]:
                    self.genes[gene_id][id] = { "mp_category" : category, "genotype_phenotype" : list() }
                self.genes[gene_id][id]["genotype_phenotype"].append({
                    "subject.symbol" : row["evidence.baseAnnotations.subject.symbol"],
                    "subject_background" : row["evidence.baseAnnotations.subject.background.name"],
                    "subject_zygosity" : row["evidence.baseAnnotations.subject.zygosity"],
                    "mp_identifier" : row["ontologyTerm.identifier"],
                    "mp_label" : row["ontologyTerm.name"] })

                print row["subject.primaryIdentifier"], row["subject.symbol"], \
                    row["evidence.baseAnnotations.subject.symbol"], \
                    row["evidence.baseAnnotations.subject.background.name"], \
                    row["evidence.baseAnnotations.subject.zygosity"], row["ontologyTerm.identifier"], \
                    row["ontologyTerm.name"]

            time.sleep(3)
            #self.logger.debug(row["subject.primaryIdentifier"], row["subject.symbol"], \
            #   row["evidence.baseAnnotations.subject.symbol"], \
            #    row["evidence.baseAnnotations.subject.background.name"], \
            #    row["evidence.baseAnnotations.subject.zygosity"], row["ontologyTerm.identifier"], \
            #    row["ontologyTerm.name"])

def main():

    obj = MouseminePhenotypeETL()
    obj.get_genotype_phenotype()

if __name__ == "__main__":
    main()
