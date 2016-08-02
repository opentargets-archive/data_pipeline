import os
import re
import sys
reload(sys);
sys.setdefaultencoding("utf8");
import pysftp
import gzip
from paramiko import AuthenticationException
import opentargets.model.core as opentargets
import logging
import json
from common import Actions
from SPARQLWrapper import SPARQLWrapper, JSON
from settings import Config
from tqdm import tqdm
from datetime import datetime, date

__author__ = 'gautierk'

logger = logging.getLogger(__name__)

TOP_LEVELS = '''
PREFIX obo: <http://purl.obolibrary.org/obo/>
select *
FROM <http://purl.obolibrary.org/obo/hp.owl>
FROM <http://purl.obolibrary.org/obo/mp.owl>
where {
  ?top_level rdfs:subClassOf <%s> .
  ?top_level rdfs:label ?top_level_label
}
'''

DIRECT_ANCESTORS = '''
# %s
PREFIX obo: <http://purl.obolibrary.org/obo/>
SELECT ?dist1 as ?distance ?y as ?ancestor ?ancestor_label ?x as ?direct_child ?direct_child_label
FROM <http://purl.obolibrary.org/obo/hp.owl>
FROM <http://purl.obolibrary.org/obo/mp.owl>
   WHERE
    {
       ?x rdfs:subClassOf ?y
       option(transitive, t_max(1), t_in(?x), t_out(?y), t_step("step_no") as ?dist1) .
       ?y rdfs:label ?ancestor_label .
       ?x rdfs:label ?direct_child_label .
       FILTER (?x = <%s>)
    }
order by ?dist1
'''

INDIRECT_ANCESTORS = '''
PREFIX obo: <http://purl.obolibrary.org/obo/>
SELECT ?dist1 as ?distance ?y as ?ancestor ?ancestor_label ?z as ?direct_child ?direct_child_label
FROM <http://purl.obolibrary.org/obo/hp.owl>
FROM <http://purl.obolibrary.org/obo/mp.owl>
   WHERE
    {
       ?x rdfs:subClassOf ?y
       option(transitive, t_max(20), t_in(?x), t_out(?y), t_step("step_no") as ?dist1) .
       ?y rdfs:label ?ancestor_label .
       ?z rdfs:subClassOf ?y .
       ?z rdfs:label ?direct_child_label .
       {SELECT ?z WHERE { ?x2 rdfs:subClassOf ?z option(transitive) FILTER (?x2 = <%s>) }}
       FILTER (?x = <%s>)
    }
order by ?dist1
'''

SPARQL_PATH_QUERY = '''
PREFIX efo: <http://www.ebi.ac.uk/efo/>
SELECT ?node_uri ?parent_uri ?parent_label ?dist ?path
FROM <http://www.ebi.ac.uk/efo/>
WHERE
  {
    {
      SELECT *
      WHERE
        {
          ?node_uri rdfs:subClassOf ?y .
          ?node_uri rdfs:label ?parent_label
        }
    }
    OPTION ( TRANSITIVE, t_min(1), t_in (?y), t_out (?node_uri), t_step (?y) as ?parent_uri, t_step ('step_no') as ?dist, t_step ('path_id') as ?path ) .
    FILTER ( ?y = efo:EFO_0000408 )
  }
'''

'''
PREFIX obo: <http://purl.obolibrary.org/obo/>

select ?superclass where {
  obo:HP_0003074 rdfs:subClassOf* ?superclass
}
'''

SINGLE_CLASS_PATH_QUERY = '''
PREFIX obo: <http://purl.obolibrary.org/obo/>

select ?class ?parent_label count(?mid) AS ?count
FROM <http://purl.obolibrary.org/obo/hp.owl>
where {
  obo:HP_0003074 rdfs:subClassOf* ?mid .
  ?mid rdfs:subClassOf* ?class .
  ?class rdfs:label ?parent_label .
}
group by ?class ?parent_label
order by ?count
'''

class OntologyActions(Actions):
    PHENOTYPESLIM = 'phenotypeslim'

class PhenotypeSlim():

    def __init__(self, sparql):

        self.sparql = sparql

        self.phenotype_current = {}
        self.phenotype_obsolete = {}
        self.phenotype_map = {}
        self.phenotype_top_levels = {}
        self.phenotype_excluded = set()

        self.disease_current = {}
        self.disease_obsolete = {}
        self.disease_map = {}
        self.disease_top_levels = {}
        self.disease_excluded = set()

        self._remote_filenames = dict()
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_ontology_top_levels(self, base_class, top_level_map):
        sparql_query = TOP_LEVELS
        self.sparql.setQuery(sparql_query%base_class)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
        for result in results["results"]["bindings"]:
            #print json.dumps(result)
            top_level_label = result['top_level_label']['value']
            top_level = result['top_level']['value']
            top_level_map[top_level] = top_level_label
            print "%s %s"%(top_level, top_level_label)

    def get_ontology_path(self, base_class, term):

        if term in self.phenotype_map or term in self.disease_map:
            return

        # if it's an orphanet term, we need to get the definition
        # and add it to the root. However, with the current level of
        # confidence we have on the data, it's better to reject them for the time
        # being

        #if term == 'http://purl.obolibrary.org/obo/HP_0001251':
        if True:

            for sparql_query in [DIRECT_ANCESTORS, INDIRECT_ANCESTORS]:
                self.sparql.setQuery(sparql_query%(term, term))
                self.sparql.setReturnFormat(JSON)
                results = None
                n = 0
                while (n < 2):
                    try:
                        results = self.sparql.query().convert()
                        n = 3
                    except SPARQLWrapper.SPARQLExceptions.EndPointNotFound, e:
                        print e
                        self.logger.error(e)
                        if n > 2:
                            raise e
                        else:
                            n=n+1

                #print len(results)
                #print json.dumps(results)

                for result in results["results"]["bindings"]:
                    #print json.dumps(result)
                    count = int(result['distance']['value'])
                    parent_label = result['ancestor_label']['value']
                    ancestor = result['ancestor']['value']
                    direct_child = result['direct_child']['value']
                    direct_child_label = result['direct_child_label']['value']
                    if direct_child not in self.phenotype_map:
                        self.phenotype_map[direct_child] = { 'label': direct_child_label , 'superclasses': [] }
                    if ancestor not in self.phenotype_map[direct_child]['superclasses']:
                        self.phenotype_map[direct_child]['superclasses'].append(ancestor)
                        print "%i %s %s (direct child is %s %s)"%(count, parent_label, ancestor, direct_child_label, direct_child)
                        print "---------"
                    #print "%i %s %s (direct child is %s %s)"%(count, parent_label, ancestor, direct_child_label, direct_child)


    def load_ontology(self, prefix='', name_space='', base_class=None, current=None, obsolete=None):
        '''
        Load ontology to accept phenotype terms that are not
        :return:
        '''
        sparql_query = '''
        %s
        SELECT DISTINCT ?ont_node ?label
        FROM %s
        {
        ?ont_node rdfs:subClassOf* <%s> .
        ?ont_node rdfs:label ?label
        }
        '''
        self.sparql.setQuery(sparql_query % (prefix, name_space, base_class))
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()

        for result in results["results"]["bindings"]:
            uri = result['ont_node']['value']
            label = result['label']['value']
            current[uri] = label
            # print(json.dumps(result, indent=4))
            # print("%s %s"%(uri, label))

        sparql_query = '''
        PREFIX oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
        PREFIX obo: <http://purl.obolibrary.org/obo/>
        SELECT DISTINCT ?hp_node ?label ?id ?hp_new
         FROM %s
         FROM <http://purl.obolibrary.org/obo/>
         {
            ?hp_node owl:deprecated true .
            ?hp_node oboInOwl:id ?id .
            ?hp_node obo:IAO_0100001 ?hp_new .
            ?hp_node rdfs:label ?label

         }
        '''
        self.sparql.setQuery(sparql_query % name_space)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()

        obsolete_classes = {}

        for result in results["results"]["bindings"]:
            uri = result['hp_node']['value']
            label = result['label']['value']
            id = result['label']['value']
            hp_new = result['hp_new']['value']
            new_label = ''
            if (not re.match('http:\/\/purl.obolibrary\.org', hp_new)):
                hp_new = "http://purl.obolibrary.org/obo/%s"%hp_new.replace(':','_')
            obsolete_classes[uri] = hp_new
        for uri in obsolete_classes:
            next_uri = obsolete_classes[uri]
            while next_uri in obsolete_classes:
                next_uri = obsolete_classes[next_uri]
            new_label = current[next_uri]
            obsolete[uri] = "Use %s label:%s"%(next_uri, new_label)
            print "%s %s"%(uri, obsolete[uri])

    def load_hpo(self, base_class):
        '''
        Load HPO to accept phenotype terms that are not in EFO
        :return:
        '''
        self.load_ontology(
            name_space='<http://purl.obolibrary.org/obo/hp.owl>',
            base_class=base_class,
            current=self.phenotype_current,
            obsolete=self.phenotype_obsolete)
        self.get_ontology_top_levels(base_class, top_level_map=self.phenotype_top_levels)

    def load_mp(self, base_class):
        '''
        Load MP to accept phenotype terms that are not in EFO
        :return:
        '''
        self.load_ontology(
            name_space='<http://purl.obolibrary.org/obo/mp.owl>',
            base_class=base_class,
            current=self.phenotype_current,
            obsolete=self.phenotype_obsolete)
        self.get_ontology_top_levels(base_class, top_level_map=self.phenotype_top_levels)

    def load_efo(self, base_class):
        '''
        Load EFO to accept rare disease terms that are not in EFO
        :return:
        '''
        self.load_ontology(
            prefix='PREFIX efo: <http://www.ebi.ac.uk/efo/>',
            name_space='<http://www.ebi.ac.uk/efo/>',
            base_class=base_class,
            current=self.disease_current,
            obsolete=self.disease_obsolete)
        self.get_ontology_top_levels(base_class, top_level_map=self.disease_top_levels)

    def exclude_phenotypes(self, l):
        '''
        :param l:
        :return:
        '''
        for p in l:
            if p not in self.phenotype_excluded:
                self.phenotype_excluded.add(p)
                print "Excluding %s"%p
                # get parents
                sparql_query = DIRECT_ANCESTORS
                self.sparql.setQuery(sparql_query%(p, p))
                self.sparql.setReturnFormat(JSON)
                results = self.sparql.query().convert()
                al = []
                for result in results["results"]["bindings"]:
                    count = int(result['distance']['value'])
                    parent_label = result['ancestor_label']['value']
                    ancestor = result['ancestor']['value']
                    al.append(ancestor)
                    self.exclude_phenotypes(al)

    def generate_ttl_query(self, filename):

        with open(filename, 'w') as hfile:
            # create restricted list
            print ",".join(self.phenotype_top_levels.keys())
            for p in self.phenotype_top_levels:
                if p in self.phenotype_map:
                    self.exclude_phenotypes(self.phenotype_map[p]['superclasses'])
            # return

            hfile.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\n")

            for k, v in self.phenotype_map.iteritems():
                count = 0
                if k not in self.phenotype_excluded:
                    hfile.write("<%s> rdfs:label \"%s\" .\n" % (k, v['label']))
                    if k in self.phenotype_top_levels:
                        hfile.write("<%s> rdfs:subClassOf <http://www.ebi.ac.uk/efo/EFO_0000651> .\n" % k)
                    else:
                        for p in v['superclasses']:
                            hfile.write("<%s> rdfs:subClassOf <%s> .\n" % (k, p))

        hfile.close()

    def _store_remote_filename(self, filename):
        print "%s" % filename
        self.logger.debug("%s" % filename)
        if filename.startswith('/upload/submissions/') and \
            filename.endswith('.json.gz'):
            self.logger.debug("%s" % filename)
            if True:
                version_name = filename.split('/')[3].split('.')[0]
                print "%s" % filename
                if '-' in version_name:
                    user, day, month, year = version_name.split('-')
                    if '_' in user:
                        datasource = ''.join(user.split('_')[1:])
                        user = user.split('_')[0]
                    else:
                        datasource = Config.DATASOURCE_INTERNAL_NAME_TRANSLATION_REVERSED[user]
                    release_date = date(int(year), int(month), int(day))

                    if user not in self._remote_filenames:
                        self._remote_filenames[user]={datasource : dict(date = release_date,
                                                                          file_path = filename,
                                                                          file_version = version_name)
                                                      }
                    elif datasource not in self._remote_filenames[user]:
                        self._remote_filenames[user][datasource] = dict(date=release_date,
                                                                        file_path=filename,
                                                                        file_version=version_name)
                    else:
                        if release_date > self._remote_filenames[user][datasource]['date']:
                            self._remote_filenames[user][datasource] = dict(date=release_date,
                                                                file_path=filename,
                                                                file_version=version_name)
            #except e:
            #    self.logger.error("%s Error checking file %s: %s" % (self.__class__.__name__, filename, e))
            #    print 'error getting remote file%s'%filename
            #    self.logger.debug('error getting remote file%s'%filename)

    def _callback_not_used(self, path):
        self.logger.debug("skipped "+path)

    def create_phenotype_slim(self):

        self.load_hpo(base_class='http://purl.obolibrary.org/obo/HP_0000118')
        self.load_mp(base_class='http://purl.obolibrary.org/obo/MP_0000001')
        self.load_efo(base_class='http://www.ebi.ac.uk/efo/EFO_0000508')

        for u in tqdm(Config.ONTOLOGY_PREPROCESSING_FTP_ACCOUNTS,
                         desc='scanning ftp accounts',
                         leave=False):
            try:
                p = Config.EVIDENCEVALIDATION_FTP_ACCOUNTS[u]
                self.logger.info("%s %s"%(u, p))
                cnopts = pysftp.CnOpts()
                cnopts.hostkeys = None  # disable host key checking.
                with pysftp.Connection(host=Config.EVIDENCEVALIDATION_FTP_HOST['host'],
                                       port=Config.EVIDENCEVALIDATION_FTP_HOST['port'],
                                       username=u,
                                       password=p,
                                       cnopts = cnopts,
                                       ) as srv:
                    srv.walktree('/', fcallback=self._store_remote_filename, dcallback=self._callback_not_used, ucallback=self._callback_not_used)
                    srv.close()
                    for datasource, file_data in tqdm(self._remote_filenames[u].items(),
                                                      desc='scanning available datasource for account %s'%u,
                                                      leave=False,):
                        latest_file = file_data['file_path']
                        file_version = file_data['file_version']
                        self.logger.info("found latest file %s for datasource %s"%(latest_file, datasource))
                        self.parse_gzipfile(latest_file, u, p)
            except AuthenticationException:
                self.logger.error('cannot connect with credentials: user:%s password:%s' % (u, p))

        self.generate_ttl_query(Config.ONTOLOGY_SLIM_FILE)

    def parse_gzipfile(self, file_path, u, p):
        print "---->%s"%file_path
        self.logger.info("%s %s" % (u, p))
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  # disable host key checking.
        with pysftp.Connection(host=Config.EVIDENCEVALIDATION_FTP_HOST['host'],
                               port=Config.EVIDENCEVALIDATION_FTP_HOST['port'],
                               username=u,
                               password=p,
                               cnopts=cnopts,
                               ) as srv:

            file_stat = srv.stat(file_path)
            file_size, file_mod_time = file_stat.st_size, file_stat.st_mtime

            with srv.open(file_path, mode='rb', bufsize=1) as f:
                with gzip.GzipFile(filename=file_path.split('/')[1],
                                   mode='rb',
                                   fileobj=f,
                                   mtime=file_mod_time) as fh:

                    logging.info('Starting parsing %s' %file_path)

                    line_buffer = []
                    offset = 0
                    chunk = 1
                    line_number = 0

                    for line in fh:
                        python_raw = json.loads(line)
                        obj = None
                        data_type = python_raw['type']
                        if data_type in Config.EVIDENCEVALIDATION_DATATYPES:
                            if data_type == 'genetic_association':
                                obj = opentargets.Genetics.fromMap(python_raw)
                            elif data_type == 'rna_expression':
                                obj = opentargets.Expression.fromMap(python_raw)
                            elif data_type in ['genetic_literature', 'affected_pathway', 'somatic_mutation']:
                                obj = opentargets.Literature_Curated.fromMap(python_raw)
                                if data_type == 'somatic_mutation' and not isinstance(python_raw['evidence']['known_mutations'],
                                                                                      list):
                                    mutations = copy.deepcopy(python_raw['evidence']['known_mutations'])
                                    python_raw['evidence']['known_mutations'] = [mutations]
                                    # self.logger.error(json.dumps(python_raw['evidence']['known_mutations'], indent=4))
                                    obj = opentargets.Literature_Curated.fromMap(python_raw)
                            elif data_type == 'known_drug':
                                obj = opentargets.Drug.fromMap(python_raw)
                            elif data_type == 'literature':
                                obj = opentargets.Literature_Mining.fromMap(python_raw)
                            elif data_type == 'animal_model':
                                obj = opentargets.Animal_Models.fromMap(python_raw)

                        if obj.disease.id:
                            for id in obj.disease.id:
                                if re.match('http://purl.obolibrary.org/obo/HP_\d+', id):
                                    ''' get all terms '''
                                    self.get_ontology_path('http://purl.obolibrary.org/obo/HP_0000118', id)

                                elif re.match('http://purl.obolibrary.org/obo/MP_\d+', id):
                                    ''' get all terms '''
                                    self.get_ontology_path('http://purl.obolibrary.org/obo/MP_0000001', id)
                                elif re.match('http://www.orpha.net/ORDO/Orphanet_\d+', id):
                                    ''' just map to the genetic disorders '''
                                    self.get_ontology_path('http://www.ebi.ac.uk/efo/EFO_0000508', id)

                fh.close()
            srv.close()
        return
