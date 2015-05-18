from datetime import datetime
import logging
from sqlalchemy import and_
from common import Actions
from common.ElasticsearchLoader import JSONObjectStorage
import requests
from common.PGAdapter import *
from StringIO import StringIO
import csv
from zipfile import ZipFile
from common.DataStructure import JSONSerializable

__author__ = 'andreap'



class HPAActions(Actions):
    DOWNLOAD='download'
    PROCESS='process'
    UPLOAD='upload'


class HPAExpression(JSONSerializable):
    def __init__(self, gene):
        self.gene = gene
        self.tissues = {}
        self.cell_lines = {}

    def get_id(self):
        return self.gene



class HPADataDownloader():


    def __init__(self, adapter):
        self.adapter = adapter
        self.session = adapter.session


    def retrieve_all(self):

        self.retrieve_normal_tissue_data()
        self.retrieve_cancer_data()
        self.retrieve_rna_data()
        self.retrieve_subcellular_location_data()


    def _download_data(self, url):
        r = requests.get(url)
        if r.status_code == 200:
            zipped_data = ZipFile(StringIO(r.content))
            info = zipped_data.getinfo(zipped_data.filelist[0].orig_filename)
            return zipped_data.open(info)
        else:
            raise Exception("failed to download data from url: %s. Status code: %i"%(url,r.status_code) )

    def retrieve_normal_tissue_data(self):
        data =  self._download_data(Config.HPA_NORMAL_TISSUE_URL)
        self._load_normal_tissue_data_to_pg(data)

    def retrieve_rna_data(self):
        data = self._download_data(Config.HPA_RNA_URL)
        self._load_rna_data_to_pg(data)

    def retrieve_cancer_data(self):
        data =  self._download_data(Config.HPA_CANCER_URL)
        self._load_cancer_data_to_pg(data)

    def retrieve_subcellular_location_data(self):
        data = self._download_data(Config.HPA_SUBCELLULAR_LOCATION_URL)
        self._load_subcellular_location_data_to_pg(data)

    def _get_csv_reader(self, csvfile):
        return csv.DictReader(csvfile)

    def _load_normal_tissue_data_to_pg(self, data):
        reader= self._get_csv_reader(data)
        rows_deleted= self.session.query(HPANormalTissue).delete()
        if rows_deleted:
            logging.info('deleted %i rows from hpa_normal_tissue'%rows_deleted)
        # '''very fast, don't use the orm functions'''
        # self.adapter.engine.execute(
        #     HPANormalTissue.__table__.insert(),
        #     [{"tissue": row['Tissue'],
        #       "cell_type": row['Cell type'],
        #       "level": row['Level'],
        #       "reliability": row['Reliability'],
        #       "gene": row['Gene'],
        #       "expression_type": row['Expression type'],
        #       } for row in reader]
        # )
        c=0
        for row in reader:
            c+=1
            self.session.add(HPANormalTissue(tissue=row['Tissue'],
                                             cell_type=row['Cell type'],
                                             level=row['Level'],
                                             reliability=row['Reliability'],
                                             gene=row['Gene'],
                                             expression_type=row['Expression type'],
                                             ))
            if c % 10000 == 0:
                logging.info("%i rows uploaded to hpa_normal_tissue"%c)
                self.session.flush()
        self.session.commit()
        logging.info('inserted %i rows in hpa_normal_tissue'%c)

    def _load_rna_data_to_pg(self, data):
        reader= self._get_csv_reader(data)
        rows_deleted= self.session.query(HPARNA).delete()
        if rows_deleted:
            logging.info('deleted %i rows from hpa_rna'%rows_deleted)
        c=0
        for row in reader:
            c+=1
            self.session.add(HPARNA(sample=row['Sample'],
                                     abundance=row['Abundance'],
                                     unit=row['Unit'],
                                     value=row['Value'],
                                     gene=row['Gene'],
                                     ))

            if c % 10000 == 0:
                logging.info("%i rows uploaded to hpa_rna"%c)
                self.session.flush()
        self.session.commit()
        logging.info('inserted %i rows in hpa_rna'%c)

    def _load_cancer_data_to_pg(self, data):
        reader= self._get_csv_reader(data)
        rows_deleted= self.session.query(HPACancer).delete()
        if rows_deleted:
            logging.info('deleted %i rows from hpa_cancer'%rows_deleted)
        c=0
        for row in reader:
            c+=1
            self.session.add(HPACancer(tumor=row['Tumor'],
                                       level=row['Level'],
                                       count_patients=row['Count patients'],
                                       total_patients=row['Total patients'],
                                       gene=row['Gene'],
                                       expression_type=row['Expression type'],
                                       ))
            if c % 10000 == 0:
                logging.info("%i rows uploaded to hpa_cancer"%c)
                self.session.flush()
        self.session.commit()
        logging.info('inserted %i rows in hpa_cancer'%c)

    def _load_subcellular_location_data_to_pg(self, data):
        reader= self._get_csv_reader(data)
        rows_deleted= self.session.query(HPASubcellularLocation).delete()
        if rows_deleted:
            logging.info('deleted %i rows from hpa_subcellular_location'%rows_deleted)
        c=0
        for row in reader:
            c+=1
            self.session.add(HPASubcellularLocation(main_location=row['Main location'],
                                       other_location=row['Other location'],
                                       gene=row['Gene'],
                                       expression_type=row['Expression type'],
                                       reliability=row['Reliability'],
                                       ))
            if c % 10000 == 0:
                logging.info("%i rows uploaded to hpa_subcellular_location"%c)
                self.session.flush()
        self.session.commit()
        logging.info('inserted %i rows in hpa_subcellular_location'%c)



class HPAProcess():

    def __init__(self, adapter):
        self.adapter = adapter
        self.session = adapter.session
        self.data ={}
        self.set_translations()

    def process_all(self):

        self.process_normal_tissue()
        self.process_rna()
        self.process_cancer()
        self.process_subcellular_location()
        self.store_data()


    def _get_available_genes(self, table = HPANormalTissue):
        # genes =[ row.gene for row in self.session.query(table).distinct(table.gene).group_by(table.gene)]
        genes =[row.gene for row in self.session.query(table.gene).distinct()]
        logging.debug('found %i genes in table %s'%(len(genes),table.__tablename__))
        return genes

    def process_normal_tissue(self):
        for gene in self._get_available_genes( HPANormalTissue):
            if gene not in self.data:
                self.init_gene(gene)
            self.data[gene]['expression'].tissues = self.get_normal_tissue_data_for_gene(gene)
        return

    def _get_row_as_dict(row):
        d = row.__dict__
        d.pop('_sa_instance_state')
        return d


    def process_rna(self):
        for gene in self._get_available_genes(HPARNA):
            if gene not in self.data:
                self.init_gene(gene)
            self.data[gene]['expression'].tissues,\
                self.data[gene]['expression'].cell_lines= self.get_rna_data_for_gene(gene)
        return

    def process_cancer(self):
        pass

    def process_subcellular_location(self):
        pass

    def store_data(self):
        if self.data.values()[0]['expression']:
            rows_deleted= self.session.query(
                ElasticsearchLoad).filter(
                    and_(ElasticsearchLoad.index==Config.ELASTICSEARCH_EXPRESSION_INDEX_NAME,
                         ElasticsearchLoad.type==Config.ELASTICSEARCH_EXPRESSION_DOC_NAME)).delete()
            if rows_deleted:
                logging.info('deleted %i rows of expression data from elasticsearch_load'%rows_deleted)
            c=0
            for gene, data in self.data.items():
                c+=1
                self.session.add(ElasticsearchLoad(id=gene,
                                                   index=Config.ELASTICSEARCH_EXPRESSION_INDEX_NAME,
                                                   type=Config.ELASTICSEARCH_EXPRESSION_DOC_NAME,
                                                   data=data['expression'].to_json(),
                                                   active=True,
                                                   date_created=datetime.now(),
                                                   date_modified=datetime.now(),
                                                  ))
                if c % 10000 == 0:
                    logging.info("%i rows of expression data inserted to elasticsearch_load"%c)
                    self.session.flush()
            self.session.commit()
            logging.info('inserted %i rows of expression data inserted in elasticsearch_load'%c)
        if self.data.values()[0]['cancer']:
            pass
        if self.data.values()[0]['subcellular_location']:
            pass

    def init_gene(self, gene):
        self.data[gene]=dict(expression = HPAExpression(gene),
                             cancer = {},#TODO
                             subcellular_location = {}, #TODO
                            )

    def get_normal_tissue_data_for_gene(self, gene):
        tissue_data = {}
        for row in self.session.query(HPANormalTissue).filter_by(gene=gene).all():
            tissue = row.tissue.replace('1','').replace('2','').strip()
            if tissue not in tissue_data:
                tissue_data[tissue]= {'protein':{
                                            'cell_type' : {},
                                            'level': 0,
                                            'expression_type':'',
                                            'reliability' : False,
                                            },

                                      'rna':{
                                      },
                                      'efo_code' : self.tissue_translation[tissue]}
            if row.cell_type not in tissue_data[tissue]['protein']['cell_type']:
                tissue_data[tissue]['protein']['cell_type'][row.cell_type] = []
            tissue_data[tissue]['protein']['cell_type'][row.cell_type].append(dict(level = self.level_translation[row.level],
                                                          expression_type = row.expression_type,
                                                          reliability = self.reliability_translation[row.reliability],
                                                          ))
            if self.level_translation[row.level] > tissue_data[tissue]['protein']['level']:
                tissue_data[tissue]['protein']['level']=self.level_translation[row.level]#TODO: improvable by giving higher priority to reliable annotations over uncertain
            if not tissue_data[tissue]['protein']['expression_type']:
                tissue_data[tissue]['protein']['expression_type'] = row.expression_type
            if self.reliability_translation[row.reliability]:
                tissue_data[tissue]['protein']['reliability'] = True

        return tissue_data

    def get_rna_data_for_gene(self, gene):
        tissue_data = self.data[gene]['expression'].tissues
        cell_line_data = {}
        if not tissue_data:
            tissue_data = {}
        for row in self.session.query(HPARNA).filter_by(gene=gene).all():
            sample = row.sample
            is_cell_line = sample not in self.tissue_translation.keys()
            if is_cell_line:
                if sample not in cell_line_data:
                     cell_line_data[sample] =  {'rna':{},
                                        }
                cell_line_data[sample]['rna']['level']=self.level_translation[row.abundance]
                cell_line_data[sample]['rna']['value']=row.value
                cell_line_data[sample]['rna']['unit']=row.unit
            else:
                if sample not in tissue_data:
                    tissue_data[sample]= {'protein':{
                                            'cell_type' : {},
                                            'level': 0,
                                            'expression_type':'',
                                            'reliability' : False,
                                            },

                                      'rna':{
                                      },
                                      'efo_code' : self.tissue_translation[sample]}
                tissue_data[sample]['rna']['level']=self.level_translation[row.abundance]
                tissue_data[sample]['rna']['value']=row.value
                tissue_data[sample]['rna']['unit']=row.unit
        return tissue_data, cell_line_data






    def set_translations(self):
        self.level_translation={'Not detected':0,
                                'Low':1,
                                'Medium':2,
                                'High':3,
                                }
        self.reliability_translation={'Supportive':True,
                                'Uncertain':False,
                                }

        self.tissue_translation ={
            'adrenal gland': 'CL_0000336',
            'appendix': 'EFO_0000849',
            'bone marrow': 'UBERON_0002371',
            'breast': 'UBERON_0000310',
            'bronchus': 'UBERON_0002185',
            'cerebellum': 'UBERON_0002037',
            'cerebral cortex': 'UBERON_0000956',
            'cervix, uterine': 'EFO_0000979',
            'colon': 'UBERON_0001155',
            'duodenum': 'UBERON_0002114',
            'endometrium': 'UBERON_0001295',
            'epididymis': 'UBERON_0001301',
            'esophagus': 'UBERON_0001043',
            'fallopian tube': 'UBERON_0003889',
            'gallbladder': 'UBERON_0002110',
            'heart muscle': 'UBERON_0002349',
            'hippocampus': 'EFO_0000530',
            'kidney': 'UBERON_0002113',
            'lateral ventricle': 'EFO_0001961',
            'liver': 'UBERON_0002107',
            'lung': 'UBERON_0002048',
            'lymph node': 'UBERON_0000029',
            'nasopharynx':'nasopharynx', #TODO: nothing matching except nasopharynx cancers
            'oral mucosa': 'UBERON_0003729',
            'ovary': 'EFO_0000973',
            'pancreas': 'UBERON_0001264',
            'parathyroid gland': 'CL_0000446',
            'placenta': 'UBERON_0001987',
            'prostate': 'UBERON_0002367',
            'rectum': 'UBERON_0001052',
            'salivary gland': 'UBERON_0001044',
            'seminal vesicle': 'UBERON_0000998',
            'skeletal muscle': 'CL_0000188',
            'skin': 'EFO_0000962',
            'small intestine': 'UBERON_0002108',
            'smooth muscle': 'EFO_0000889',
            'soft tissue': 'soft_tissue',#TODO: cannot map automatically to anything except: EFO_0000691 that is sarcoma (and includes soft tissue tumor)
            'spleen': 'UBERON_0002106',
            'stomach': 'UBERON_0000945',
            'testis': 'UBERON_0000473',
            'thyroid gland': 'UBERON_0002046',
            'tonsil': 'UBERON_0002372',
            'urinary bladder': 'UBERON_0001255',
            'vagina': 'UBERON_0000996',
            'adipose tissue': 'adipose tissue',
            }


class HPAUploader():

    def __init__(self, adapter,loader):
        self.adapter = adapter
        self.session = adapter.session
        self.loader = loader

    def upload_all(self):
        JSONObjectStorage.refresh_index_data_in_es(self.loader,
                                         self.session,
                                         Config.ELASTICSEARCH_EXPRESSION_INDEX_NAME,
                                         Config.ELASTICSEARCH_EXPRESSION_DOC_NAME)
