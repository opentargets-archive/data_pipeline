from collections import OrderedDict
import copy
from datetime import datetime
import logging
from StringIO import StringIO
import urllib2
from sqlalchemy import and_
import ujson as json
from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage
from common.PGAdapter import HgncGeneInfo, EnsemblGeneInfo, UniprotInfo, ElasticsearchLoad
from common.UniprotIO import UniprotIterator
from settings import Config

__author__ = 'andreap'

UNI_ID_ORG_PREFIX = 'http://identifiers.org/uniprot/'
ENS_ID_ORG_PREFIX = 'http://identifiers.org/ensembl/'

class GeneActions(Actions):
    MERGE='merge'
    UPLOAD='upload'

class Gene(JSONSerializable):
    def __init__(self, id=None):

        self.id = id
        self.hgnc_id = None
        self.approved_symbol = ""
        self.approved_name = ""
        self.status = ""
        self.locus_group = ""
        self.previous_symbols = []
        self.previous_names = []
        self.symbol_synonyms = []
        self.name_synonyms = []
        self.chromosome = ""
        self.enzyme_ids = []
        self.entrez_gene_id = ""
        self.ensembl_gene_id = ""
        self.refseq_ids = []
        self.gene_family_tag = ""
        self.gene_family_description = ""
        self.ccds_ids = []
        self.vega_ids = []
        self.alias_name=[]
        self.alias_symbol=[]
        self.pubmed_ids =[]
        self.is_active_in_ensembl = False
        self.ensembl_assembly_name = ""
        self.biotype = ""
        self.ensembl_description = ""
        self.gene_end = None
        self.gene_start = None
        self.strand = None
        self.ensembl_external_name = ""
        self.ensembl_gene_version = None
        self.cytobands = ""
        self.ensembl_release = None
        self.uniprot_id = ""
        self.uniprot_mapping_date = None
        self.uniprot_accessions = []
        self.is_in_swissprot = False
        self.dbxrefs = []
        self.uniprot_function = []
        self.uniprot_keywords = []
        self.uniprot_similarity = []
        self.uniprot_subunit = []
        self.uniprot_subcellular_location = []
        self.uniprot_pathway = []
        self.reactome = []
        self.go =[]
        self.pdb = []
        self.chembl = []
        self.drugbank = []
        self.pfam = []
        self.interpro = []
        self.is_ensembl_reference = []


    def _set_id(self):
        if self.ensembl_gene_id:
            self.id = self.ensembl_gene_id
        elif self.hgnc_id:
            self.id = self.hgnc_id
        elif self.entrez_gene_id:
            self.id = self.entrez_gene_id
        else:
            self.id = None


    def load_hgnc_data(self, data):
        if data.hgnc_id:
            self.hgnc_id = data.hgnc_id
        if data.approved_symbol:
            self.approved_symbol = data.approved_symbol
        if data.approved_name:
            self.approved_name = data.approved_name
        if data.status:
            self.status = data.status
        if data.locus_group:
            self.locus_group = data.locus_group
        if data.previous_symbols:
            self.previous_symbols = data.previous_symbols.split(', ')
        if data.previous_names:
            self.previous_names = data.previous_names.split(', ')
        if data.synonyms:
            self.symbol_synonyms = data.synonyms.split(', ')
        if data.name_synonyms:
            self.name_synonyms = data.name_synonyms.split(', ')
        # if data.chromosome :
        # self.chromosome = data.chromosome
        if data.enzyme_ids:
            self.enzyme_ids = data.enzyme_ids.split(', ')
        if data.entrez_gene_id:
            self.entrez_gene_id = data.entrez_gene_id
        if data.ensembl_gene_id:
            self.ensembl_gene_id = data.ensembl_gene_id
            if not self.ensembl_gene_id:
                self.ensembl_gene_id = data.ensembl_id_supplied_by_ensembl
        if data.refseq_ids:
            self.refseq_ids = data.refseq_ids.split(', ')
        if data.gene_family_tag:
            self.gene_family_tag = data.gene_family_tag
        if data.gene_family_description:
            self.gene_family_description = data.gene_family_description
        if data.ccds_ids:
            self.ccds_ids = data.ccds_ids.split(', ')
        if data.vega_ids:
            self.vega_ids = data.vega_ids.split(', ')

    def load_hgnc_data_from_json(self, data):

        if 'ensembl_gene_id' in data:
            self.ensembl_gene_id = data['ensembl_gene_id']
            if not self.ensembl_gene_id:
                self.ensembl_gene_id = data['ensembl_id_supplied_by_ensembl']
            if 'hgnc_id' in data:
                self.hgnc_id = data['hgnc_id']
            if 'symbol' in data:
                self.approved_symbol = data['symbol']
            if 'name' in data:
                self.approved_name = data['name']
            if 'status' in data:
                self.status = data['status']
            if 'locus_group' in data:
                self.locus_group = data['locus_group']
            if 'prev_symbols' in data:
                self.previous_symbols = data['prev_symbols']
            if 'prev_names' in data:
                self.previous_names = data['prev_names']
            if 'alias_symbol' in data:
                self.symbol_synonyms = data['alias_symbol']
            if 'alias_name' in data:
                self.name_synonyms = data['alias_name']
            if 'enzyme_ids' in data:
                self.enzyme_ids = data['enzyme_ids']
            if 'entrez_id' in data:
                self.entrez_gene_id = data['entrez_id']
            if 'refseq_accession' in data:
                self.refseq_ids = data['refseq_accession']
            if 'gene_family_tag' in data:
                self.gene_family_tag = data['gene_family_tag']
            if 'gene_family_description' in data:
                self.gene_family_description = data['gene_family_description']
            if 'ccds_ids' in data:
                self.ccds_ids = data['ccds_ids']
            if 'vega_id' in data:
                self.vega_ids = data['vega_id']
            if 'uniprot_ids' in data:
                self.uniprot_accessions = data['uniprot_ids']
                self.uniprot_id = self.uniprot_accessions [0]
            if 'pubmed_id' in data:
                self.pubmed_ids = data['pubmed_id']

    def load_ensembl_data(self, data):

        if data.ensembl_gene_id:
            self.is_active_in_ensembl = True
            self.ensembl_gene_id = data.ensembl_gene_id
        if data.assembly_name:
            self.ensembl_assembly_name = data.assembly_name
        if data.biotype:
            self.biotype = data.biotype
        if data.description:
            self.ensembl_description = data.description.split(' [')[0]
            if not self.approved_name:
                self.approved_name= self.ensembl_description
        if data.gene_end is not None:
            self.gene_end = data.gene_end
        if data.gene_start is not None:
            self.gene_start = data.gene_start
        if data.strand is not None:
            self.strand = data.strand
        if data.chromosome:
            self.chromosome = data.chromosome
        if data.external_name:
            self.ensembl_external_name = data.external_name
            if not self.approved_symbol:
                self.approved_symbol= data.external_name
        if data.gene_version is not None:
            self.ensembl_gene_version = data.gene_version
        if data.cytobands:
            self.cytobands = data.cytobands
        if data.ensembl_release:
            self.ensembl_release = data.ensembl_release
        self.is_ensembl_reference = data.is_reference


    def load_uniprot_entry(self, seqrec):
        self.uniprot_id = seqrec.name
        self.is_in_swissprot = True
        if seqrec.dbxrefs:
            self.dbxrefs.extend(seqrec.dbxrefs)
            self.dbxrefs= sorted(list(set(self.dbxrefs)))
        for k, v in seqrec.annotations.items():
            if k == 'accessions':
                self.uniprot_accessions = v
            if k == 'keywords':
                self.uniprot_keywords = v
            if k == 'comment_function':
                self.uniprot_function = v
            if k == 'comment_similarity':
                self.uniprot_similarity = v
            if k == 'comment_subunit':
                self.uniprot_subunit = v
            if k == 'comment_subcellularlocation_location':
                self.uniprot_subcellular_location = v
            if k == 'comment_pathway':
                self.uniprot_pathway = v
            if k == 'gene_name_primary':
                if not self.approved_symbol:
                    self.approved_symbol= v
                elif v!= self.approved_symbol:
                    if v not in self.symbol_synonyms:
                        self.symbol_synonyms.append(v)
            if k == 'gene_name_synonym':
                if v not in self.symbol_synonyms:
                    self.symbol_synonyms.append(v)
            if k.startswith('recommendedName'):
                self.name_synonyms.extend(v)
            if k.startswith('alternativeName'):
                self.name_synonyms.extend(v)
        self.name_synonyms.append(seqrec.description)
        self.name_synonyms = list(set(self.name_synonyms))
        if 'GO' in  seqrec.annotations['dbxref_extended']:
            self.go = seqrec.annotations['dbxref_extended']['GO']
        if 'Reactome' in  seqrec.annotations['dbxref_extended']:
            self.reactome = seqrec.annotations['dbxref_extended']['Reactome']
        if 'PDB' in  seqrec.annotations['dbxref_extended']:
            self.pdb = seqrec.annotations['dbxref_extended']['PDB']
        if 'ChEMBL' in  seqrec.annotations['dbxref_extended']:
            self.chembl = seqrec.annotations['dbxref_extended']['ChEMBL']
        if 'DrugBank' in  seqrec.annotations['dbxref_extended']:
            self.drugbank = seqrec.annotations['dbxref_extended']['DrugBank']
        if 'Pfam' in  seqrec.annotations['dbxref_extended']:
            self.pfam = seqrec.annotations['dbxref_extended']['Pfam']
        if 'InterPro' in  seqrec.annotations['dbxref_extended']:
            self.interpro = seqrec.annotations['dbxref_extended']['InterPro']

    def get_id_org(self):
        return ENS_ID_ORG_PREFIX + self.ensembl_gene_id

    def create_suggestions(self):

        field_order = [self.approved_symbol,
                       self.approved_name,
                       self.symbol_synonyms,
                       self.name_synonyms,
                       self.previous_symbols,
                       self.previous_names,
                       self.uniprot_id,
                       self.uniprot_accessions,
                       self.ensembl_gene_id,
                       self.entrez_gene_id,
                       self.refseq_ids
                       ]

        self._private = {'suggestions' : dict(input = [],
                                              output = self.approved_symbol,
                                              payload = dict(gene_id = self.id,
                                                             gene_symbol = self.approved_symbol,
                                                             gene_name = self.approved_name),
                                              )
        }

        for field in field_order:
            if isinstance(field, list):
                self._private['suggestions']['input'].extend(field)
            else:
                self._private['suggestions']['input'].append(field)
        self._private['suggestions']['input'] = [x.lower() for x in self._private['suggestions']['input']]


class GeneSet():
    def __init__(self):
        self.genes = OrderedDict()

    def __contains__(self, item):
        return self.genes.__contains__(item)
        # if item:
        # if item in self.genes:
        #         return True
        #     # else:
        #     #     for gene in self.genes.values():
        #     #         if (item == gene.ensembl_gene_id) or \
        #     #                 (item == gene.uniprot_id) or \
        #     #                 (item == gene.hgnc_id):
        #     #             return True
        # return False

    def remove_gene(self,key):
        del self.genes[key]


    def add_gene(self, gene):
        if isinstance(gene, Gene):
            if not gene.id:
                gene._set_id()
            if gene.id:
                self.genes[gene.id] = gene

    def get_gene(self, geneid):
        try:
            return copy.deepcopy(self.genes[geneid])
        except KeyError:
            return copy.deepcopy(self.genes[geneid.replace(ENS_ID_ORG_PREFIX, '')])

    def iterate(self):
        for k, v in self.genes.items():
            yield k, v

    def __len__(self):
        return len(self.genes)

    def get_stats(self):
        stats = '''%i Genes Parsed:
\t%i (%1.1f%%) with Ensembl ID
\t%i (%1.1f%%) with HGNC ID
\t%i (%1.1f%%) with other IDs
\t%i (%1.1f%%) with Uniprot IDs
\t%i (%1.1f%%) with a Uniprot reviewed entry
\t%i (%1.1f%%) active genes in Emsembl'''

        ens, hgnc, other, ens_active, uni, swiss = 0., 0., 0., 0., 0., 0.

        for geneid, gene in self.genes.items():
            if geneid.startswith('ENS'):
                ens += 1
            elif geneid.startswith('HGNC:'):
                hgnc += 1
            else:
                other += 1

            if gene.is_active_in_ensembl:
                ens_active += 1
            if gene.uniprot_id:
                uni += 1
            if gene.is_in_swissprot:
                swiss += 1

        tot = len(self)
        stats = stats % (tot,
                         ens, ens / tot * 100.,
                         hgnc, hgnc / tot * 100.,
                         other, other / tot * 100.,
                         uni, uni / tot * 100.,
                         swiss, swiss / tot * 100.,
                         ens_active, ens_active / tot * 100.)
        return stats



class GeneManager():
    """
    Merge data available in postgres into proper json objects
    """

    def __init__(self,
                 adapter):

        self.adapter=adapter
        self.session=adapter.session
        self.genes = GeneSet()



    def merge_all(self):
        # self._get_hgnc_data()
        self._get_hgnc_data_from_json()
        self._get_ensembl_data()
        self._get_uniprot_data()
        self._store_data()

    def _get_hgnc_data(self):
        for row in self.session.query(HgncGeneInfo).yield_per(1000):
            if not '~' in row.approved_symbol:
                gene = Gene()
                gene.load_hgnc_data(row)
                self.genes.add_gene(gene)

        logging.info("STATS AFTER HGNC PARSING:\n" + self.genes.get_stats())

    def _get_hgnc_data_from_json(self):
        req = urllib2.Request('ftp://ftp.ebi.ac.uk/pub/databases/genenames/new/json/hgnc_complete_set.json')
        response = urllib2.urlopen(req)
        # print response.code
        # if response.code == '200':
        data = json.loads(response.read())
        for row in data['response']['docs']:
            gene = Gene()
            gene.load_hgnc_data_from_json(row)
            self.genes.add_gene(gene)

        logging.info("STATS AFTER HGNC PARSING:\n" + self.genes.get_stats())


    def _get_ensembl_data(self):
        for row in self.session.query(EnsemblGeneInfo).yield_per(1000):
            if row.ensembl_gene_id in self.genes:
                gene = self.genes.get_gene(row.ensembl_gene_id)
                gene.load_ensembl_data(row)
                self.genes.add_gene(gene)
            else:
                gene = Gene()
                gene.load_ensembl_data(row)
                self.genes.add_gene(gene)

        self._clean_non_reference_genes()

        logging.info("STATS AFTER ENSEMBL PARSING:\n" + self.genes.get_stats())

    def _clean_non_reference_genes(self):
        for geneid, gene in self.genes.iterate():
            if not gene.is_ensembl_reference:
                self.genes.remove_gene(geneid)

    def _get_uniprot_data(self):
        c = 0
        for row in self.session.query(UniprotInfo).yield_per(1000):
            seqrec = UniprotIterator(StringIO(row.uniprot_entry), 'uniprot-xml').next()
            c += 1
            if c % 5000 == 0:
                logging.info("%i entries retrieved for uniprot" % c)
            if 'Ensembl' in seqrec.annotations['dbxref_extended']:
                ensembl_data=seqrec.annotations['dbxref_extended']['Ensembl']
                ensembl_genes_id=[]
                for enst in ensembl_data:
                    ensembl_genes_id.append(ensembl_data[enst]['gene ID'])
                ensembl_genes_id = list(set(ensembl_genes_id))
                success = False
                for ensembl_id in ensembl_genes_id:
                    if ensembl_id in self.genes:
                        gene = self.genes.get_gene(ensembl_id)
                        gene.load_uniprot_entry(seqrec)
                        self.genes.add_gene(gene)
                        success=True
                        break
                if not success:
                    logging.debug('Cannot find ensembl id(s) %s coming from uniprot entry %s in available geneset' % (ensembl_genes_id, seqrec.id))
            else:
                logging.debug('Cannot find ensembl mapping in the uniprot entry %s' % seqrec.id)
        logging.info("%i entries retrieved for uniprot" % c)

        logging.info("STATS AFTER UNIPROT MAPPING:\n" + self.genes.get_stats())



    def _store_data(self):
        rows_deleted= self.session.query(
                ElasticsearchLoad).filter(
                    and_(ElasticsearchLoad.index==Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                         ElasticsearchLoad.type==Config.ELASTICSEARCH_GENE_NAME_DOC_NAME)).delete()
        if rows_deleted:
            logging.info('deleted %i rows of gene data from elasticsearch_load'%rows_deleted)
        c=0
        for geneid, gene in self.genes.iterate():
            if gene.is_ensembl_reference:
                c+=1
                self.session.add(ElasticsearchLoad(id=gene.id,
                                                   index=Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                                                   type=Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                                                   data=gene.to_json(),
                                                   active=True,
                                                   date_created=datetime.now(),
                                                   date_modified=datetime.now(),
                                                  ))
                if c % 10000 == 0:
                    logging.info("%i rows of gene data inserted to elasticsearch_load"%c)
                    self.session.flush()
        self.session.commit()
        logging.info('inserted %i rows of gene data inserted in elasticsearch_load'%c)




class GeneUploader():
    """upload the gene objects to elasticsearch"""

    def __init__(self,
                 adapter,
                 loader):
        self.adapter=adapter
        self.session=adapter.session
        self.loader=loader


    def upload_all(self):
        JSONObjectStorage.refresh_index_data_in_es(self.loader,
                                         self.session,
                                         Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                                         Config.ELASTICSEARCH_GENE_NAME_DOC_NAME)


class GeneRetriever():
    """
    Will retrieve a Gene object form the processed json stored in postgres
    """
    def __init__(self,
                 adapter):
        self.adapter=adapter
        self.session=adapter.session

    def get_gene(self, geneid):
        json_data = JSONObjectStorage.get_data_from_pg(self.session,
                                                       Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                                                       Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                                                       geneid)
        gene = Gene(geneid)
        gene.load_json(json_data)
        return gene