import logging
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.Settings import Config
import mrtarget.cfg

class Metrics:
    def __init__(self, es):
        self.logger = logging.getLogger(__name__)
        self.esquery = ESQuery(es)
        self.filename = mrtarget.cfg.Configuration().args.metric_file

    def generate_metrics(self):
        self.logger.info("Producing data release metrics")

        count_drug_w_evidence = self.esquery.count_drug_w_evidence()
        count_entity_w_association = self.esquery.count_entity_w_association()
        count_target_w_symbol = self.esquery.count_target_w_symbol()
        count_target_w_mp = self.esquery.count_target_w_mp()
        count_target_w_hallmark = self.esquery.count_target_w_hallmark()
        count_target_w_biomarker = self.esquery.count_target_w_biomarker()
        count_BRAF_evidence = self.esquery.count_BRAF_evidence()
        count_withdrawn_drug_evidence = self.esquery.count_withdrawn_drug_evidence()
        count_trinucleotide_evidence = self.esquery.count_trinucleotide_evidence()

        count_datatype_evidence = self.esquery.count_datatype_evidence()
        count_datatype_association = self.esquery.count_datatype_association()

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
                "evidence link to trinucleotide expansion:\t" + str(count_trinucleotide_evidence['hits']['total']) + "\n"
            )

            for ds in Config.DATASOURCE_TO_DATATYPE_MAPPING.iterkeys():
                count_datasource_evidence = self.esquery.count_datasource_evidence(ds)
                metrics_output.write("evidence from datasource " + ds + ":\t" + str(count_datasource_evidence['hits']['total']) + "\n")

            for item in count_datatype_evidence['aggregations']['datatypes']['buckets']:
                datatype = item['key']
                evidence_count = item['doc_count']
                metrics_output.write("evidence from datatype " + datatype + ":\t" + str(evidence_count) + "\n")

            for item in count_datatype_association['aggregations']['datatypes']['buckets']:
                datatype = item['key']
                association_count = item['doc_count']
                metrics_output.write("association from datatype " + datatype + ":\t" + str(association_count) + "\n")

        metrics_output.close()
        self.logger.info("Producing data release metrics - Completed")






