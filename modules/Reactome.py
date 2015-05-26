import logging
import pprint
import requests
from sqlalchemy.exc import IntegrityError
from common import Actions
from common.DataStructure import TreeNode, JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage
from common.PGAdapter import ReactomePathwayData, ReactomePathwayRelation, ReactomeEnsembleMapping
from settings import Config
import networkx as nx
from networkx.readwrite import json_graph
from networkx.algorithms import all_simple_paths
from networkx.convert import to_dict_of_dicts, to_dict_of_lists

__author__ = 'andreap'


class ReactomeActions(Actions):
    DOWNLOAD='download'
    PROCESS='process'
    UPLOAD='upload'


class ReactomeNode(TreeNode, JSONSerializable):

    def __init__(self, **kwargs):
        super(ReactomeNode, self).__init__(**kwargs)


class ReactomeDataDownloader():
    allowed_species =['Homo sapiens']


    def __init__(self, adapter):
        self.adapter = adapter
        self.session = adapter.session


    def retrieve_all(self):
        self.delete_old_data()
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
        added_relations=[]
        for row in data.split('\n'):
            if row:
                ensembl_id, reactome_id,url, name, eco, species  = row.split('\t')
                relation =(ensembl_id, reactome_id)
                if relation not in added_relations:
                    if (reactome_id in self.valid_pathway_ids) and (species in self.allowed_species):
                        self.session.add(ReactomeEnsembleMapping(ensembl_id=ensembl_id,
                                                                 reactome_id=reactome_id,
                                                                 evidence_code=eco,
                                                                 species=species
                                                             ))
                        added_relations.append(relation)
                        if len(added_relations)% 1000 == 0:
                            logging.info("%i rows inserted to reactome_ensembl_mapping"%len(added_relations))
                            self.session.flush()
                else:
                    logging.warn("Pathway mapping %s is already loaded, skipping duplicate data"%str(relation))
        self.session.commit()
        logging.info('inserted %i rows in reactome_ensembl_mapping'%len(added_relations))

    def _load_pathway_relation_to_pg(self, data):
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

    def delete_old_data(self):
        rows_deleted= self.session.query(ReactomePathwayRelation).delete()
        if rows_deleted:
            logging.info('deleted %i rows from reactome_pathway_relation'%rows_deleted)
        rows_deleted= self.session.query(ReactomeEnsembleMapping).delete()
        if rows_deleted:
            logging.info('deleted %i rows from reactome_ensembl_mapping'%rows_deleted)
        rows_deleted= self.session.query(ReactomePathwayData).delete()
        if rows_deleted:
            logging.info('deleted %i rows from reactome_pathway_data'%rows_deleted)


class ReactomeProcess():

    def __init__(self, adapter):
        self.adapter = adapter
        self.session = adapter.session
        self.g = nx.DiGraph(name="reactome")
        self.data = {}

    def process_all(self):
        self._process_pathway_hierarchy()
        self._store_data()

    def _process_pathway_hierarchy(self):
        root = 'root'
        self.relations =dict()
        self.g.add_node(root, name = "", species = "")
        for row in self.session.query(ReactomePathwayData):
            self.g.add_node(row.id, name = row.name, species = row.species)
        children = set()
        for row in self.session.query(ReactomePathwayRelation):
            self.g.add_edge(row.id, row.child)
            children.add(row.child)
        nodes_without_parent=set(self.g.nodes())-children
        for node in nodes_without_parent:
            if node != root:
                self.g.add_edge(root, node)
        for node, node_data in self.g.nodes(data=True):
            if node != root:
                ancestors = set()
                paths = list(all_simple_paths(self.g, root, node))
                for path in paths:
                    for p in path:
                        ancestors.add(p)
                self.data[node]=(ReactomeNode(id = node,
                                              label = node_data['name'],
                                              path = paths,
                                              children = self.g.successors(node),
                                              parents = self.g.predecessors(node),
                                              is_root = node == root,
                                              ancestors = list(ancestors)
                                              ))



    def _store_data(self):
        JSONObjectStorage.store_to_pg(self.session,
                                              Config.ELASTICSEARCH_REACTOME_INDEX_NAME,
                                              Config.ELASTICSEARCH_REACTOME_REACTION_DOC_NAME,
                                              self.data)

    

class ReactomeUploader():

    def __init__(self,
                 adapter,
                 loader):
        self.adapter=adapter
        self.session=adapter.session
        self.loader=loader

    def upload_all(self):
        JSONObjectStorage.refresh_index_data_in_es(self.loader,
                                         self.session,
                                         Config.ELASTICSEARCH_REACTOME_INDEX_NAME,
                                         Config.ELASTICSEARCH_REACTOME_REACTION_DOC_NAME,
                                         )



class ReactomeRetriever():
    """
    Will retrieve a Reactome object form the processed json stored in postgres
    """
    def __init__(self,
                 adapter):
        self.adapter=adapter
        self.session=adapter.session

    def get_reaction(self, reaction_id):
        json_data = JSONObjectStorage.get_data_from_pg(self.session,
                                                       Config.ELASTICSEARCH_REACTOME_INDEX_NAME,
                                                       Config.ELASTICSEARCH_REACTOME_REACTION_DOC_NAME,
                                                       reaction_id)
        reaction = ReactomeNode()
        reaction.load_json(json_data)
        return reaction