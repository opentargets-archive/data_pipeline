import gzip
import logging
import json
from settings import Config
import opentargets.model.core as opentargets
import opentargets.model.evidence.association_score as evidence_association_score

__author__ = "Gautier Koscielny"
__copyright__ = "Copyright 2014-2016, Open Targets"
__credits__ = []
__license__ = "Apache 2.0"
__version__ = ""
__maintainer__ = "Gautier Koscielny"
__email__ = "gautierk@opentargets.org"
__status__ = "Production"

from logging.config import fileConfig

logger = logging.getLogger('root')


class EvidenceStringReader(object):
    def __init__(self):
        pass

    def parse_gzipfile(self, filename, mode, out_file):

        out_fh = open(out_file, 'w')

        with gzip.GzipFile(filename=filename,
                           mode=mode) as fh:

            logging.info('Starting parsing %s' % filename)

            line_buffer = []
            offset = 0
            chunk = 1
            line_number = 0

            for line in fh:
                out_line = line.rstrip()
                python_raw = json.loads(line)
                obj = None
                data_type = python_raw['type']
                if data_type in Config.EVIDENCEVALIDATION_DATATYPES:
                    if data_type == 'genetic_association':
                        obj = opentargets.Genetics.fromMap(python_raw)

                        '''
                        check the evidence string
                        '''
                        if obj.evidence.gene2variant.resource_score is None:
                            resource_score = evidence_association_score.Probability(
                                type="probability",
                                method=evidence_association_score.Method(
                                    description="NA",
                                    reference="NA",
                                    url="NA"),
                                value=1)
                            obj.evidence.gene2variant.resource_score = resource_score
                            out_line = obj.to_JSON(indentation=None)
                        else:
                            print obj.evidence.gene2variant.resource_score.value
                out_fh.write(out_line + "\n")
        fh.close()
        out_fh.close()

def main():

    obj = EvidenceStringReader()
    obj.parse_gzipfile(filename='/Users/koscieln/Documents/data/ftp/cttv012/upload/submissions/cttv012-22-11-2016.json.gz', mode='rb', out_file='/Users/koscieln/Documents/data/ftp/cttv012/upload/submissions/cttv012-28-11-2016.json')


if __name__ == "__main__":
    main()
