import logging
import requests
from sqlalchemy.exc import IntegrityError
from common import Actions
from common.DataStructure import NetworkNode, JSONSerializable
from common.PGAdapter import ReactomePathwayData, ReactomePathwayRelation
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
    allowed_species =['Homo sapiens']


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
        rows_deleted= self.session.query(ReactomePathwayData).delete()
        if rows_deleted:
            logging.info('deleted %i rows from reactome_pathway_data'%rows_deleted)
        self.valid_pathway_ids=[]
        for row in data.split('\n'):
            if row:
                pathway_id, pathway_name, species = row.split('\t')
                if pathway_id not in self.valid_pathway_ids:
                    if species in self.allowed_species:
                        self.session.add(ReactomePathwayData(id=pathway_id,
                                                             name=pathway_name,
                                                             species=species,
                                                            ))
                        self.valid_pathway_ids.append(pathway_id)
                        if len(self.valid_pathway_ids) % 1000 == 0:
                            logging.info("%i rows inserted to reactome_pathway_data"%len(self.valid_pathway_ids))
                            self.session.flush()
                else:
                    logging.warn("Pathway id %s is already loaded, skipping duplicate data"%pathway_id)
        self.session.commit()
        logging.info('inserted %i rows in reactome_pathway_data'%len(self.valid_pathway_ids))

    def _load_pathway_gene_mappings_to_pg(self, data):
        pass

    def _load_pathway_relation_to_pg(self, data):
        rows_deleted= self.session.query(ReactomePathwayRelation).delete()
        if rows_deleted:
            logging.info('deleted %i rows from reactome_pathway_relation'%rows_deleted)
        added_relations=[]
        for row in data.split('\n'):
            if row:
                parent_id, child_id = row.split('\t')
                relation =(parent_id, child_id)
                if relation not in added_relations:
                    if parent_id in self.valid_pathway_ids:
                        self.session.add(ReactomePathwayRelation(id=parent_id,
                                                                 child=child_id,
                                                             ))
                        added_relations.append(relation)
                        if len(added_relations)% 1000 == 0:
                            logging.info("%i rows inserted to reactome_pathway_relation"%len(added_relations))
                            self.session.flush()
                else:
                    logging.warn("Pathway relation %s is already loaded, skipping duplicate data"%str(relation))
        self.session.commit()
        logging.info('inserted %i rows in reactome_pathway_relation'%len(added_relations))
