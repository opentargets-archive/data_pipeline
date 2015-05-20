from sqlalchemy.dialects.postgresql import JSONB

from settings import Config
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL
from sqlalchemy import Column, Integer, String, Date, Text,TIMESTAMP, BOOLEAN, Float
from sqlalchemy.ext.declarative import declarative_base


__author__ = 'andreap'

Base = declarative_base()






class Adapter():
    '''Adapter to the postgres database'''
    def __init__(self, database_url = Config.POSTGRES_DATABASE):
        self.engine = self.db_connect(database_url)
        self.setup()

    def db_connect(self,url):
        """
        Performs database connection using database settings.
        Returns sqlalchemy engine instance
        """
        return create_engine(URL(**url))



    def setup(self,):

        Base.metadata.bind = self.engine
        DBSession = sessionmaker(bind=self.engine)
        self.session = DBSession()


    def close(self):
        try:
            self.session.close()
        except:
            pass


'''TABLES'''
class LatestEvidenceString(Base):
    __tablename__ = 'vw_evidence_strings_latest'
    __table_args__ = {'schema':'public'}
    uniq_assoc_fields_hashdig = Column(String(250), primary_key=True)
    json_doc_hashdig = Column(String(250))
    evidence_string = Column(JSONB)
    data_source_name = Column(Text)
    json_doc_version = Column(String(250))
    json_schema_version = Column(Integer)
    release_date = Column(Date)

class ECOPath(Base):
    __tablename__ = 'eco_path'
    __table_args__ = {'schema':'rdf_conversion'}
    # id = Column(Integer, primary_key=True)
    uri = Column(Text,primary_key=True)
    tree_path = Column(Text)
    # uri_id_org = Column(Text)

class EFOPath(Base):
    __tablename__ = 'efo_path'
    __table_args__ = {'schema':'rdf_conversion'}
    uri = Column(Text)
    tree_path = Column(JSONB)
    id = Column(Integer, primary_key=True)

class EFONames(Base):
    __tablename__ = 'efo_names'
    __table_args__ = {'schema':'rdf_conversion'}
    uri = Column(Text, primary_key=True)
    # uri_id_org = Column(Text)
    label = Column(Text)
    synonyms = Column(Text)
    description = Column(Text)

class EFOFirstChild(Base):
    __tablename__ = 'efo_first_child_node'
    __table_args__ = {'schema':'rdf_conversion'}
    id = Column(Integer, primary_key=True)
    parent_uri = Column(Text)
    first_child_uri = Column(Text)

class HgncGeneInfo(Base):
    __tablename__ = 'hgnc_gene_info'
    __table_args__ = {'schema':'lookups'}
    hgnc_id = Column(Text, primary_key=True)
    approved_symbol = Column(Text)
    approved_name = Column(Text)
    status = Column(Text)
    locus_group = Column(Text)
    previous_symbols = Column(Text)
    previous_names = Column(Text)
    synonyms = Column(Text)
    name_synonyms = Column(Text)
    chromosome = Column(Text)
    accession_numbers = Column(Text)
    enzyme_ids = Column(Text)
    entrez_gene_id = Column(Text)
    ensembl_gene_id = Column(Text)
    mouse_genome_database_id = Column(Text)
    pubmed_ids = Column(Text)
    refseq_ids = Column(Text)
    gene_family_tag = Column(Text)
    gene_family_description = Column(Text)
    record_type = Column(Text)
    primary_ids = Column(Text)
    secondary_ids = Column(Text)
    ccds_ids = Column(Text)
    vega_ids = Column(Text)
    locus_specific_databases = Column(Text)
    ensembl_id_supplied_by_ensembl = Column(Text)

class EnsemblGeneInfo(Base):
    __tablename__ = 'ensembl_gene_info'
    __table_args__ = {'schema':'lookups'}
    ensembl_gene_id = Column(Text, primary_key=True)
    assembly_name = Column(Text)
    biotype = Column(Text)
    description = Column(Text)
    gene_end = Column(Integer)
    external_name = Column(Text)
    logic_name = Column(Text)
    chromosome = Column(Text)
    gene_start = Column(Integer)
    strand = Column(Integer)
    source = Column(Text)
    gene_version = Column(Integer)
    cytobands = Column(Text)
    ensembl_release = Column(Integer)
    is_reference = Column(BOOLEAN)

# class EnsemblToUniprotMapping(Base):
#     __tablename__ = 'uniprot_ensembl_mapping'
#     __table_args__ = {'schema':'lookups'}
#     uniprot_ensembl_mapping_id = Column(Integer, primary_key=True)
#     uniprot_accession = Column(Text)
#     uniprot_entry_type = Column(Integer)# 1:uniprot, 0:trembl
#     ensembl_transcript_id = Column(Text)
#     ensembl_protein_id = Column(Text)
#     ensembl_gene_id = Column(Integer)
#     uniprot_note = Column(Text)
#     download_date = Column(Date)

class UniprotInfo(Base):
    __tablename__ = 'uniprot_info'
    __table_args__ = {'schema':'lookups'}
    uniprot_accession = Column(Text, primary_key=True)
    uniprot_entry = Column(Text)

class ElasticsearchLoad(Base):
    __tablename__ = 'elasticsearch_load'
    __table_args__ = {'schema':'pipeline'}
    id = Column(Text, primary_key=True)
    index = Column(Text, primary_key=True)
    type = Column(Text)
    data = Column(JSONB)
    date_created = Column(TIMESTAMP)
    date_modified = Column(TIMESTAMP)
    active = Column(BOOLEAN)
    successfully_loaded = Column(BOOLEAN)

class EvidenceValidation(Base):
    __tablename__ = 'evidence_validation'
    __table_args__ = {'schema':'pipeline'}
    provider_id = Column(Text, primary_key=True)
    filename = Column(Text, primary_key=True)
    md5 = Column(Text)
    date_created = Column(TIMESTAMP)
    date_modified = Column(TIMESTAMP)
    date_validated = Column(TIMESTAMP)
    nb_submission = Column(Integer)
    successfully_validated = Column(BOOLEAN)

class HPANormalTissue(Base):
    __tablename__ = 'hpa_normal_tissue'
    __table_args__ = {'schema':'pipeline'}
    gene = Column(Text, primary_key=True)
    tissue = Column(Text, primary_key=True)
    cell_type = Column(Text, primary_key=True)
    level = Column(Text)
    expression_type = Column(Text)
    reliability = Column(Text)


class HPACancer(Base):
    __tablename__ = 'hpa_cancer'
    __table_args__ = {'schema':'pipeline'}
    gene = Column(Text, primary_key=True)
    tumor = Column(Text, primary_key=True)
    level = Column(Text, primary_key=True)
    count_patients = Column(Integer)
    total_patients = Column(Integer)
    expression_type = Column(Text)

class HPARNA(Base):
    __tablename__ = 'hpa_rna'
    __table_args__ = {'schema':'pipeline'}
    gene = Column(Text, primary_key=True)
    sample = Column(Text, primary_key=True)
    value = Column(Float)
    unit = Column(Text)
    abundance = Column(Text)

class HPASubcellularLocation(Base):
    __tablename__ = 'hpa_subcellular_location'
    __table_args__ = {'schema':'pipeline'}
    gene = Column(Text, primary_key=True)
    main_location = Column(Text)
    other_location = Column(Text)
    expression_type = Column(Text)
    reliability = Column(Text)


class ReactomePathwayData(Base):
    __tablename__ = 'reactome_pathway_data'
    __table_args__ = {'schema':'pipeline'}
    id = Column(Text, primary_key=True)
    name = Column(Text)
    species = Column(Text)

class ReactomePathwayRelation(Base):
    __tablename__ = 'reactome_pathway_relation'
    __table_args__ = {'schema':'pipeline'}
    id = Column(Text, ForeignKey('pipeline.reactome_pathway_data.id'), primary_key=True)
    child = Column(Text, ForeignKey('pipeline.reactome_pathway_data.id'), primary_key=True)

class ReactomeEnsembleMapping(Base):
    __tablename__ = 'reactome_ensembl_mapping'
    __table_args__ = {'schema':'pipeline'}
    ensembl_id = Column(Text, primary_key=True)
    reactome_id = Column(Text, ForeignKey('pipeline.reactome_pathway_data.id'), primary_key=True)
    evidence_code = Column(Text)
    species = Column(Text)