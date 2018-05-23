import logging
from collections import Counter
from pprint import pprint

from elasticsearch import helpers
from tqdm import tqdm
from mrtarget.common import TqdmToLogger
from mrtarget.common import Actions
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.Settings import Config

class Metrics(Actions):
    def __init__(self, es):
        self.logger = logging.getLogger(__name__)
        self.es = es
        self.esquery = ESQuery(es)
        self.filename = Config.METRICS_FILENAME
        tqdm_out = TqdmToLogger(self.logger, level=logging.INFO)

    def generate_metrics(self):
        self.logger.info("Generating data release metrics")

        self.logger.info("Producing 'drugs (unique) with evidence'...")
        drug_count_with_evidence = self.esquery.count_drug_with_evidence()

        self.logger.info("Producing 'evidence link to withdrawn drug'...")
        evidence_count_withdrawn_drug = self.esquery.count_evidence_with_withdrawn_drug()

        with open(self.filename, 'w') as metrics_output:

            metrics_output.write(
                "drugs(unique) with evidence:\t" + str(drug_count_with_evidence['aggregations']['general_drug']['value']) + "\n")
            metrics_output.write(
                "evidence link to withdrawn drug:\t" + str(evidence_count_withdrawn_drug['hits']['total']) + "\n")

        metrics_output.close()


def main():
    m = Metrics()

if __name__ == "__main__":
    main()







