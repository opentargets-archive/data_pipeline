import logging
from mrtarget.common import TqdmToLogger
from mrtarget.common import Actions
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.Settings import Config

class Metrics(Actions):
    def __init__(self, es):
        self.logger = logging.getLogger(__name__)
        self.esquery = ESQuery(es)
        self.filename = Config.METRICS_FILENAME
        tqdm_out = TqdmToLogger(self.logger, level=logging.INFO)

    def generate_metrics(self):
        self.logger.info("Producing data release metrics")

        # TODO the index name is hardcoded with 18.04, needs to be parametized
        count_drug_w_evidence = self.esquery.count_drug_w_evidence()
        count_entity_w_association = self.esquery.count_entity_w_association()
        count_target_w_symbol = self.esquery.count_target_w_symbol()
        count_target_w_mp = self.esquery.count_target_w_mp()
        count_target_w_hallmark = self.esquery.count_target_w_hallmark()
        count_target_w_biomarker = self.esquery.count_target_w_biomarker()
        count_BRAF_evidence = self.esquery.count_BRAF_evidence()
        count_withdrawn_drug_evidence = self.esquery.count_withdrawn_drug_evidence()
        count_abstracted_evidence = self.esquery.count_abstracted_evidence()
        count_trinucleotide_evidence = self.esquery.count_trinucleotide_evidence()

        with open(self.filename, 'w') as metrics_output:
            metrics_output.write(
                "drugs(unique) with evidence:\t" + str(count_drug_w_evidence['aggregations']['general_drug']['value']) + "\n" +
                "diseases(unique) with association:\t" + str(count_entity_w_association['aggregations']['general_disease']['value']) + "\n" +
                "targets(unique) with association:\t" + str(count_entity_w_association['aggregations']['general_target']['value']) + "\n" +
                "targets with approved symbol:\t" + str(count_target_w_symbol['hits']['total']) + "\n" +
                "targets with mouse phenotype:\t" + str(count_target_w_mp['hits']['total']) + "\n" +
                "targets with cancer hallmark:\t" + str(count_target_w_hallmark['hits']['total']) + "\n" +
                "targets with cancer biomarker:\t" + str(count_target_w_biomarker['hits']['total']) + "\n" +
                "evidence link to BRAF:\t" + str(count_BRAF_evidence['hits']['total']) + "\n" +
                "evidence link to withdrawn drug:\t" + str(count_withdrawn_drug_evidence['hits']['total']) + "\n"
                "evidence with abstract injected:\t" + str(count_abstracted_evidence['hits']['total']) + "\n"
                "evidence link to trinucleotide expansion:\t" + str(count_trinucleotide_evidence['hits']['total']) + "\n"
            )

            for datasource in Config.DATASOURCE_TO_DATATYPE_MAPPING.iterkeys():
                count_datasource_evidence = self.esquery.count_datasource_evidence(datasource)

                metrics_output.write(
                    "evidence from " + datasource + ":\t" + str(count_datasource_evidence['hits']['total']) + "\n")

        self.logger.info("Producing data release metrics - Completed")

        metrics_output.close()







