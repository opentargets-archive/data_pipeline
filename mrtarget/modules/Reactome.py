import logging
import csv

import networkx as nx
from networkx.algorithms import all_simple_paths

from mrtarget.common.DataStructure import TreeNode, JSONSerializable
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.Settings import Config
from mrtarget.common import URLZSource

class ReactomeNode(TreeNode, JSONSerializable):
    def __init__(self, **kwargs):
        super(ReactomeNode, self).__init__(**kwargs)


class ReactomeDataDownloader():
    allowed_species = ['Homo sapiens']

    def __init__(self, pathway_data_url, pathway_relation_url):
        self.logger = logging.getLogger(__name__)
        self.pathway_data_url = pathway_data_url
        self.pathway_relation_url = pathway_relation_url

    def get_pathway_data(self):
        self.valid_pathway_ids = []
        with URLZSource(self.pathway_data_url).open() as source:
            for row in source:
                pathway_id, pathway_name, species = row.strip().split('\t')
                pathway_id = pathway_id[1:-1] if pathway_id[0] == '"' else pathway_id
                pathway_name = pathway_name[1:-1] if pathway_name[0] == '"' else pathway_name
                species = species[1:-1] if species[0] == '"' else species

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
        with URLZSource(self.pathway_relation_url).open() as source:
            for row in source:
                parent_id, child_id = row.strip().split('\t')
                parent_id = parent_id[1:-1] if parent_id[0] == '"' else parent_id
                child_id = child_id[1:-1] if child_id[0] == '"' else child_id

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
    def __init__(self, loader, pathway_data_url, pathway_relation_url):
        self.loader = loader
        self.g = nx.DiGraph(name="reactome")
        self.data = {}
        '''download data'''
        self.downloader = ReactomeDataDownloader(pathway_data_url, pathway_relation_url)
        self.logger = logging.getLogger(__name__)

    def process_all(self):
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

                #ensure these are real tuples, not generators
                #otherwise they can't be serialized to json
                children = tuple(self.g.successors(node))
                parents = tuple(self.g.predecessors(node))

                self.loader.put(index_name=Config.ELASTICSEARCH_REACTOME_INDEX_NAME,
                    doc_type=Config.ELASTICSEARCH_REACTOME_REACTION_DOC_NAME,
                    ID=node,
                    body=dict(id=node,
                        label=node_data['name'],
                        path=paths,
                        children=children,
                        parents=parents,
                        is_root=node == root,
                        ancestors=list(ancestors)
                    ))
        #make sure the index is all ready for future operations before completing this step
        self.loader.flush_all_and_wait(Config.ELASTICSEARCH_REACTOME_INDEX_NAME)

    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, esquery):
        self.logger.info("Starting QC")

        #number of reactions
        reaction_count = 0
        #Note: try to avoid doing this more than once!
        for reaction in esquery.get_all_reactions():
            reaction_count += 1

        #put the metrics into a single dict
        metrics = dict()
        metrics["reactome.count"] = reaction_count

        self.logger.info("Finished QC")
        return metrics



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
