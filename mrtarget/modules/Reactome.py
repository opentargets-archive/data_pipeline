import logging
import csv

import networkx as nx
from networkx.algorithms import all_simple_paths

from mrtarget.common.DataStructure import TreeNode, JSONSerializable
from mrtarget.common.connection import new_es_client
from mrtarget.common.esutil import ElasticsearchBulkIndexManager
from opentargets_urlzsource import URLZSource

import elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll

import simplejson as json

class ReactomeNode(TreeNode, JSONSerializable):
    def __init__(self, **kwargs):
        super(ReactomeNode, self).__init__(**kwargs)


class ReactomeDataDownloader():

    def __init__(self, pathway_data_url, pathway_relation_url):
        self.logger = logging.getLogger(__name__)
        self.allowed_species = ['Homo sapiens']
        self.headers = ["id", "description", "species"]
        self.headers_pathway_rel = ["id", "related_id"]
        self.pathway_data_url = pathway_data_url
        self.pathway_relation_url = pathway_relation_url

    def get_pathway_data(self):
        self.valid_pathway_ids = []
        with URLZSource(self.pathway_data_url).open() as source:
            for i, row in enumerate(csv.DictReader(source, fieldnames=self.headers, dialect='excel-tab'), start=1):
                if len(row) != 3:
                    raise ValueError('Reactome.py: Pathway file format unexpected at line %d.' % i)

                pathway_id = row["id"]
                pathway_name = row["description"]
                species = row["species"]

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
            for i, row in enumerate(
                    csv.DictReader(source, fieldnames=self.headers_pathway_rel, dialect='excel-tab'), start=1):
                if len(row) != 2:
                    raise ValueError('Reactome.py: Pathway Relation file format unexpected at line %d.' % i)

                parent_id = row["id"]
                child_id = row["related_id"]

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

def generate_documents(g):
    for node, node_data in g.nodes(data=True):
        if node != 'root':
            ancestors = set()
            paths = list(all_simple_paths(g, 'root', node))
            for path in paths:
                for p in path:
                    ancestors.add(p)

            #ensure these are real tuples, not generators
            #otherwise they can't be serialized to json
            children = tuple(g.successors(node))
            parents = tuple(g.predecessors(node))

            body = dict(id=node,
                label=node_data['name'],
                path=paths,
                children=children,
                parents=parents,
                is_root=node == 'root',
                ancestors=list(ancestors)
            )
            yield body

"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(reactions, index, doc):
    for reaction in reactions:
        action = {}
        action["_index"] = index
        action["_type"] = doc
        action["_id"] = reaction["id"]
        #elasticsearch client uses https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L24
        #to turn objects into JSON bodies. This in turn calls json.dumps() using simplejson if present.
        action["_source"] = reaction

        yield action

class ReactomeProcess():
    def __init__(self, es_hosts, es_index, es_doc, es_mappings, es_settings,
            pathway_data_url, pathway_relation_url,
            workers_write, queue_write):
        self.es_hosts = es_hosts
        self.es_index = es_index
        self.es_doc = es_doc
        self.es_mappings = es_mappings
        self.es_settings = es_settings
        self.downloader = ReactomeDataDownloader(pathway_data_url, pathway_relation_url)

        self.logger = logging.getLogger(__name__)
        self.g = nx.DiGraph(name="reactome")
        self.data = {}
        self.workers_write = workers_write
        self.queue_write = queue_write

    def process_all(self, dry_run):

        self.relations = dict()
        self.g.add_node('root', name="", species="")

        for row in self.downloader.get_pathway_data():
            self.g.add_node(row['id'], name=row['name'], species=row['species'])
        children = set()
        for row in self.downloader.get_pathway_relations():
            self.g.add_edge(row['id'], row['child'])
            children.add(row['child'])

        nodes_without_parent = set(self.g.nodes()) - children
        for node in nodes_without_parent:
            if node != 'root':
                self.g.add_edge('root', node)

        with URLZSource(self.es_mappings).open() as mappings_file:
            mappings = json.load(mappings_file)

        with URLZSource(self.es_settings).open() as settings_file:
            settings = json.load(settings_file)

        es = new_es_client(self.es_hosts)
        with ElasticsearchBulkIndexManager(es, self.es_index, settings, mappings):
            #write into elasticsearch
            chunk_size = 1000 #TODO make configurable
            docs = generate_documents(self.g)
            actions = elasticsearch_actions(docs, self.es_index, self.es_doc)
            failcount = 0

            if not dry_run:
                results = None
                if self.workers_write > 0:
                    results = elasticsearch.helpers.parallel_bulk(es, actions,
                            thread_count=self.workers_write,
                            queue_size=self.queue_write, 
                            chunk_size=chunk_size)
                else:
                    results = elasticsearch.helpers.streaming_bulk(es, actions,
                            chunk_size=chunk_size)
                for success, details in results:
                    if not success:
                        failcount += 1

                if failcount:
                    raise RuntimeError("%s relations failed to index" % failcount)


    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, es, index):
        self.logger.info("Starting QC")

        #number of reactions
        reaction_count = 0
        #Note: try to avoid doing this more than once!
        for reaction in Search().using(es).index(index).query(MatchAll()).scan():
            reaction_count += 1

        #put the metrics into a single dict
        metrics = dict()
        metrics["reactome.count"] = reaction_count

        self.logger.info("Finished QC")
        return metrics


