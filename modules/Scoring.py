from sqlalchemy import and_
from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage
from common.PGAdapter import ElasticsearchLoad
from settings import Config

__author__ = 'andreap'


class ScoringActions(Actions):
    PROCESS='process'
    UPLOAD='upload'

class Score(JSONSerializable):

    def __init__(self, target, disease):
        self.overall = 0.0
        self.target = target
        self.disease = disease
        self.init_scores()

    def init_scores(self):
        self.datatypes={}
        self.datasources={}

        for ds,dt in Config.DATASOURCE_TO_DATATYPE_MAPPING.items():
            self.datasources[dt]=0.0
            self.datatypes[ds]=0.0


class Scorer():

    def __init__(self):
        pass

class ScoringProcess():

    def __init__(self,
                 adapter):
        self.adapter=adapter
        self.session=adapter.session

    def process_all(self):
        self.score_target_disease_pairs()

    def score_target_disease_pairs(self):
        c=0
        for target_row in self.session.query(ElasticsearchLoad.id).filter(and_(
                        ElasticsearchLoad.index==Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                        ElasticsearchLoad.type==Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                        ElasticsearchLoad.active==True)
                    ).yield_per(10):
            target = target_row.id
            for disease_row in self.session.query(ElasticsearchLoad.id).filter(and_(
                        ElasticsearchLoad.index==Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                        ElasticsearchLoad.type==Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                        ElasticsearchLoad.active==True)
                    ).yield_per(10):
                disease = disease_row.id
                c+=1
                evidence = self._get_evidence_for_pair(target, disease)

                if evidence or (c%1000 ==0):
                    print c, target, disease, evidence
                    # exit(0)

    def _get_evidence_for_pair(self, target, disease):
        evidences = [row.data for row in self.session.query(ElasticsearchLoad.data).filter(and_(
                        ElasticsearchLoad.index.like(Config.ELASTICSEARCH_DATA_INDEX_NAME+'%'),
                        ElasticsearchLoad.active==True,
                        ElasticsearchLoad.data[('target','id')].astext == target,
                        ElasticsearchLoad.data[('disease','id')].astext == disease,

                        )

                    ).yield_per(100)]
        return len(evidences)




class ScoringUploader():

    def __init__(self,
                 adapter,
                 loader):
        self.adapter=adapter
        self.session=adapter.session
        self.loader=loader

    def upload_all(self):
        self.clear_old_data()
        JSONObjectStorage.refresh_index_data_in_es(self.loader,
                                         self.session,
                                         Config.ELASTICSEARCH_DATA_SCORE_INDEX_NAME
                                         )
        self.loader.optimize_index(Config.ELASTICSEARCH_DATA_SCORE_INDEX_NAME+'*')

    def clear_old_data(self):
        self.loader.clear_index(Config.ELASTICSEARCH_DATA_SCORE_INDEX_NAME+'*')
