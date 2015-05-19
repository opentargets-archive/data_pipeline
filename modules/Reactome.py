import requests
from common import Actions
from common.DataStructure import NetworkNode, JSONSerializable
from settings import Config

__author__ = 'andreap'


class ReactomeActions(Actions):
    DOWNLOAD='download'
    PROCESS='process'
    UPLOAD='upload'


class ReactomeNode(NetworkNode, JSONSerializable):

    def __init__(self):
        super(NetworkNode, self).__init__()


class ReactomeDataDownloader():


    def __init__(self, adapter):
        self.adapter = adapter
        self.session = adapter.session


    def retrieve_all(self):

        self.retrieve_pathway_data()
        self.retrieve_pathway_gene_mappings()
        self.retrieve_pathway_relation()


    def _download_data(self, url):
        r = requests.get(url)
        if r.status_code == 200:
            return r.content
        else:
            raise Exception("failed to download data from url: %s. Status code: %i"%(url,r.status_code) )


    def retrieve_pathway_data(self):
        data =  self._download_data(Config.REACTOME_PATHWAY_DATA)
        self._load_pathway_data_to_pg(data)

    def retrieve_pathway_gene_mappings(self):
        data =  self._download_data(Config.REACTOME_ENSEMBL_MAPPINGS)
        self._load_pathway_gene_mappings_to_pg(data)


    def retrieve_pathway_relation(self):
        data =  self._download_data(Config.REACTOME_PATHWAY_RELATION)
        self._load_pathway_relation_to_pg(data)


    def _load_pathway_data_to_pg(self, data):
        pass

    def _load_pathway_gene_mappings_to_pg(self, data):
        pass

    def _load_pathway_relation_to_pg(self, data):
        pass
