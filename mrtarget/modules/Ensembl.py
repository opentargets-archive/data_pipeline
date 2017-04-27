import json

import mysql.connector
import requests
import time

from mrtarget.common import Actions
from mrtarget.Settings import Config

'''

'''
class EnsemblActions(Actions):
    PROCESS='process'

class EnsemblMysqlGene(object):
    '''
    Use the Ensembl human core database to retrieve a list of Ensembl gene IDs.
    This list is then used by "EnsemblRestGenePost" to generate JSONs containing
    all required gene information for the CTTV pipeline.
    '''
    def __init__(self, ensembl_release):
        '''
        Instantiate with an Ensembl release number and set the MySQL connection instance.
        :param ensembl_release: int
        :return: None
        '''
        dbname = 'homo_sapiens_core_%d_38' % ensembl_release
        try:
            self.conn = mysql.connector.connect(host='ensembldb.ensembl.org', user='anonymous', port=5306, db=dbname, passwd='')
        except mysql.connector.OperationalError as mysql_ex:
            raise mysql_ex

    def get_ensembl_gene_ids(self):
        '''
        Execute an SQL statement, process its output and return a list of Ensembl stable IDs.
        :return: list
        '''
        sql = 'SELECT stable_id FROM gene'
        cursor = self.conn.cursor()
        cursor.execute(sql)
        ensembl_gene_ids = [element[0] for element in cursor.fetchall()]
        cursor.close()
        return ensembl_gene_ids
    def conn_close(self):
        '''
        Explicitly close the connection to the Ensembl MySQL database.
        :return: None
        '''
        self.conn.close()

class EnsemblRestGenePost():
    '''
    Use the Ensembl REST API obtain gene information to be used in the data pipeline.
    '''
    def __init__(self, ensembl_gene_ids):
        '''
        Initialize with a list of Ensembl gene IDs.
        :param ensembl_gene_ids: list
        :return: None
        '''
        self.ensembl_gene_ids = ensembl_gene_ids

    def __query_rest_api(self):
        '''

        :return:
        '''
        server = "http://rest.ensembl.org"
        ext = "/lookup/id"
        headers={ "Content-Type" : "application/json", "Accept" : "application/json"}
        gene_id_string_formatted_for_post = '[' + ', '.join(['"' + ensembl_gene_id + '"' for ensembl_gene_id in self.ensembl_gene_ids]) + ']'
        ids_list = '{ "ids" : %s }' % gene_id_string_formatted_for_post
        req = requests.post(server+ext, headers=headers, data=ids_list)
        return req.json()

    def get_gene_post_output(self, max_retry_count = 10):
        '''
        Calls "__query_rest_api()" in a loop to allow for
        failures. If the call does not generate an exception, the gene info JSON is returned.
        Will re-try up to the number set in "max_retry_count" and raises an
        exception when this limit is reached.
        Returns a nested dictionary, outer keys are the gene IDs, inner dictionaries contain the gene details
        :param max_retry_count:
        :return: dict
        '''
        success = False
        gene_post_output = {}
        retry_count = 0
        while success == False:
            try:
                gene_post_output = self.__query_rest_api()
                success = True
            except ValueError as ex:
                if ex.message == 'No JSON object could be decoded':
                    time.sleep(1)
                    retry_count += 1
                    if retry_count == max_retry_count:
                        raise Exception("Maximum retry count reached!")
                else:
                    raise ex
        return gene_post_output

class EnsemblGeneInfo(object):
    def __init__(self, ensembl_release):
        '''
        Set the Ensembl gene IDs list and Ensembl release verion attributes.
        :param ensembl_release: int
        :return:
        '''
        mysql_gene = EnsemblMysqlGene(ensembl_release)
        self.mysql_genes = mysql_gene.get_ensembl_gene_ids()
        self.ensembl_release = ensembl_release
        mysql_gene.conn_close()

    def __chunk_list(self, input_list, chunk_size=Config.ENSEMBL_CHUNK_SIZE):
        '''
        Breaks the input list into chunks. Used to limit the number of identifiers
        sent to Ensembl REST POST API calls (limit 1000).
        :param chunk_size: int
        :return: generator
        '''
        sublist = []
        for element in input_list:
            sublist.append(element)
            if len(sublist) == chunk_size:
                yield sublist
                sublist = []
        if sublist:
            yield sublist

    def __add_additional_info(self, gene_info_json_map):
        '''
        Inject additional information into the gene JSON returned by the REST API.
        Adds: Ensembl release number and "is_reference" flag.
        :param map: dict
        :return:
        '''
        chromosomes = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16',
                       '17', '18', '19', '20', '21', '22', 'X', 'Y', 'MT']
        new_gene_info_json_map = {}
        for ensembl_gene_id, ensembl_rest_json in gene_info_json_map.items():
            ensembl_rest_json['ensembl_release'] = self.ensembl_release
            if ensembl_rest_json['seq_region_name'] in chromosomes:
                ensembl_rest_json['is_reference'] = True
            else:
                ensembl_rest_json['is_reference'] = False
            new_gene_info_json_map[ensembl_gene_id] = ensembl_rest_json
        return new_gene_info_json_map

    def get_gene_info_json_map(self):
        '''
        Loop over a series of sub-lists of genes and instantiate "EnsemblRestGenePost for each one.
        Gather all the output into a dictionary of dictionaries. Use "__add_additional_info" to add
        the Ensembl release and "is_reference" to the inner dictionaries.
        :return: dict
        '''
        gene_info_json_map = {}
        for sublist in self.__chunk_list(self.mysql_genes):
            rest_gene_post = EnsemblRestGenePost(sublist)
            gene_post_output = rest_gene_post.get_gene_post_output()
            gene_info_json_map.update(gene_post_output)
        return self.__add_additional_info(gene_info_json_map)


class EnsemblProcess(object):

    def __init__(self, loader):
        self.loader = loader

    def process(self, ensembl_release=Config.ENSEMBL_RELEASE_VERSION):
        gene_info = EnsemblGeneInfo(ensembl_release)
        for ens_id, data in gene_info.get_gene_info_json_map().items():
            self.loader.put(Config.ELASTICSEARCH_ENSEMBL_INDEX_NAME,
                            Config.ELASTICSEARCH_ENSEMBL_DOC_NAME,
                            ens_id,
                            json.dumps(data),
                            True)
