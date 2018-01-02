from yapsy.IPlugin import IPlugin
from mrtarget.modules.GeneData import Gene
from mrtarget.Settings import Config
import urllib2
import ujson as json
from tqdm import tqdm
import logging
logging.basicConfig(level=logging.INFO)

class HGNC(IPlugin):

    def __init__(self, *args, **kwargs):
        self._logger = logging.getLogger(__name__)

    def print_name(self):
        self._logger.info("HGNC gene data plugin")

    def merge_data(self, genes, loader, r_server, tqdm_out):
        self._logger.info("HGNC parsing - requesting from URL %s" % Config.HGNC_COMPLETE_SET)
        req = urllib2.Request(Config.HGNC_COMPLETE_SET)
        response = urllib2.urlopen(req)
        self._logger.info("HGNC parsing - response code %s" % response.code)
        data = json.loads(response.read())
        for row in tqdm(data['response']['docs'],
                        desc='loading genes from HGNC',
                        unit_scale=True,
                        unit='genes',
                        file=tqdm_out,
                        leave=False):
            gene = Gene()
            gene.load_hgnc_data_from_json(row)
            genes.add_gene(gene)
        self._logger.info("STATS AFTER HGNC PARSING:\n" + genes.get_stats())
