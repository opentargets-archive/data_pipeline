import logging

import networkx as nx
import requests
from networkx.algorithms import all_simple_paths

from mrtarget.common import Actions
from mrtarget.common.DataStructure import TreeNode, JSONSerializable
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.Settings import Config

__author__ = 'andreap'


class ReactomeActions(Actions):
    PROCESS = 'process'


class ReactomeNode(TreeNode, JSONSerializable):
    def __init__(self, **kwargs):
        super(ReactomeNode, self).__init__(**kwargs)


class ReactomeDataDownloader():
    allowed_species = ['Homo sapiens']

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _download_data(self, url):
        r = requests.get(url)
        try:
            r.raise_for_status()
        except:
            raise Exception("failed to download data from url: %s. Status code: %i" % (url, r.status_code))
        return r.content

    #
    # def retrieve_pathway_gene_mappings(self):
    #     data =  self._download_data(Config.REACTOME_ENSEMBL_MAPPINGS)
    #     self._load_pathway_gene_mappings_to_pg(data)

    #
    # def _load_pathway_gene_mappings_to_pg(self, data):
    #     self.relations = {}
    #     added_relations=[]
    #     for row in data.split('\n'):
    #         if row:
    #             ensembl_id, reactome_id,url, name, eco, species  = row.split('\t')
    #             relation =(ensembl_id, reactome_id)
    #             if relation not in added_relations:
    #                 if (reactome_id in self.valid_pathway_ids) and (species in self.allowed_species):
    #                     self.relations[relation]=dict(ensembl_id=ensembl_id,
    #                                                              reactome_id=reactome_id,
    #                                                              evidence_code=eco,
    #                                                              species=species
    #                                                          )
    #                     added_relations.append(relation)
    #                     if len(added_relations)% 1000 == 0:
    #                         self.logger.info("%i rows parsed for reactome_ensembl_mapping"%len(added_relations))
    #             else:
    #                 self.logger.warn("Pathway mapping %s is already loaded, skipping duplicate data"%str(relation))
    #     self.logger.info('parsed %i rows for reactome_ensembl_mapping'%len(added_relations))


    def get_pathway_data(self):
        self.valid_pathway_ids = []
        for row in self._download_data(Config.REACTOME_PATHWAY_DATA).split('\n'):
            if row:
                pathway_id, pathway_name, species = row.split('\t')
                if pathway_id not in self.valid_pathway_ids:
                    if species in self.allowed_species:
                        self.valid_pathway_ids.append(pathway_id)
                        yield dict(id=pathway_id,
                                   name=pathway_name,
                                   species=species,
                                   )
                        if len(self.valid_pathway_ids) % 1000 == 0:
                            self.logger.debug("%i rows parsed for reactome_pathway_data" % len(self.valid_pathway_ids))
                else:
                    self.logger.warn("Pathway id %s is already loaded, skipping duplicate data" % pathway_id)
        self.logger.info('parsed %i rows for reactome_pathway_data' % len(self.valid_pathway_ids))

    def get_pathway_relations(self):
        added_relations = []
        for row in self._download_data(Config.REACTOME_PATHWAY_RELATION).split('\n'):
            if row:
                parent_id, child_id = row.split('\t')
                relation = (parent_id, child_id)
                if relation not in added_relations:
                    if parent_id in self.valid_pathway_ids:
                        yield dict(id=parent_id,
                                   child=child_id,
                                   )
                        added_relations.append(relation)
                        if len(added_relations) % 1000 == 0:
                            self.logger.debug("%i rows parsed from reactome_pathway_relation" % len(added_relations))
                else:
                    self.logger.warn("Pathway relation %s is already loaded, skipping duplicate data" % str(relation))
        self.logger.info('parsed %i rows from reactome_pathway_relation' % len(added_relations))


class ReactomeProcess():
    def __init__(self, loader):
        self.loader = loader
        self.g = nx.DiGraph(name="reactome")
        self.data = {}
        '''download data'''
        self.downloader = ReactomeDataDownloader()
        self.logger = logging.getLogger(__name__)

    def process_all(self):
        self._process_pathway_hierarchy()
        self.loader.close()

    def _process_pathway_hierarchy(self):
        root = 'root'
        self.relations = dict()
        self.g.add_node(root, name="", species="")
        for row in self.downloader.get_pathway_data():
            self.g.add_node(row['id'], name=row['name'], species=row['species'])
        children = set()
        for row in self.downloader.get_pathway_relations():
            self.g.add_edge(row['id'], row['child'])
            children.add(row['child'])
        nodes_without_parent = set(self.g.nodes()) - children
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
                self.loader.put(index_name=Config.ELASTICSEARCH_REACTOME_INDEX_NAME,
                                doc_type=Config.ELASTICSEARCH_REACTOME_REACTION_DOC_NAME,
                                ID=node,
                                body=dict(id=node,
                                          label=node_data['name'],
                                          path=paths,
                                          children=self.g.successors(node),
                                          parents=self.g.predecessors(node),
                                          is_root=node == root,
                                          ancestors=list(ancestors)
                                          ))



class ReactomeRetriever():
    """
    Will retrieve a Reactome object form the processed json stored in elasticsearch
    """

    def __init__(self,
                 es):
        self.es_query = ESQuery(es)
        self._cache = {}
        self.logger = logging.getLogger(__name__)

    def get_reaction(self, reaction_id):
        if reaction_id not in self._cache:
            reaction = ReactomeNode()
            reaction.load_json(self.es_query.get_reaction(reaction_id))
            self._cache[reaction_id] = reaction
        return self._cache[reaction_id]
