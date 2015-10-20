import logging
from sqlalchemy import and_
from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage
from common.PGAdapter import ElasticsearchLoad, TargetToDiseaseAssociationScoreMap
from modules.EvidenceString import Evidence
from settings import Config

__author__ = 'andreap'


class ScoringActions(Actions):
    EXTRACT='extract'
    PROCESS='process'
    UPLOAD='upload'


class ScoringMethods():
    HARMONIC_SUM ='harmonic-sum'
    SUM = 'sum'
    MAX = 'max'


class AssociationScore(JSONSerializable):

    def __init__(self):

        self.overall = 0.0
        self.init_scores()

    def init_scores(self):
        self.datatypes={}
        self.datasources={}

        for ds,dt in Config.DATASOURCE_TO_DATATYPE_MAPPING.items():
            self.datasources[dt]=0.0
            self.datatypes[ds]=0.0

class AssociationScoreSet(JSONSerializable):

    def __init__(self, target, disease):
        self.target = target
        self.disease = disease
        for method in ScoringMethods.__dict__.values():
            self.__dict__[method] = AssociationScore()


class Scorer():

    def __init__(self):
        pass

    def score(self,target, disease, evidence, method  = ScoringMethods.HARMONIC_SUM):

        association_score = AssociationScoreSet(target,disease)

        if method == ScoringMethods.HARMONIC_SUM:
            self._harmonic_sum(evidence, association_score)
        elif  method == ScoringMethods.SUM:
            self._sum(evidence, association_score)
        elif  method == ScoringMethods.MAX:
            self._max(evidence, association_score)
        else:
            raise NotImplementedError

    def _harmonic_sum(self, evidence, association_score):
        pass

    def _sum(self, evidence, association_score):
        pass

    def _max(self, evidence, association_score):
        pass

class ScoringExtract():

    def __init__(self,
                 adapter):
        self.adapter=adapter
        self.session=adapter.session

    def extract(self):
        '''
        iterate over all evidence strings available and extract the target to disease mappings used to calculate the scoring
        :return:
        '''

        #TODO: delete old scoring data
        rows_deleted = self.session.query(
                TargetToDiseaseAssociationScoreMap).delete(synchronize_session='fetch')

        if rows_deleted:
            logging.info('deleted %i rows from elasticsearch_load' % rows_deleted)
        #TODO: iterate over every evidence string and extract relevant data
        c=0
        for row in self.session.query(ElasticsearchLoad.data).filter(and_(
                        ElasticsearchLoad.index.like(Config.ELASTICSEARCH_DATA_INDEX_NAME+'%'),
                        ElasticsearchLoad.active==True,
                        )
                    ).yield_per(100):
            c+=1
            evidence = Evidence(row.data).evidence
            for efo in evidence['_private']['efo_codes']:
                self.session.add(TargetToDiseaseAssociationScoreMap(
                                              target_id=evidence['target']['id'],
                                              disease_id=efo,
                                              evidence_id=evidence['id'],
                                              is_direct=efo==evidence['disease']['id'],
                                              association_score=evidence['scores']['association_score'],
                                              datasource=evidence['sourceID'],
                                              ))
            if c % 100 == 0:
                self.session.flush()
            if c % 10000 == 0:
                logging.info("%i rows inserted to score-data table" %(c))

         #TODO: commit
        self.session.commit()







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
                        ElasticsearchLoad.active==True,
                        ElasticsearchLoad.id == 'ENSG00000113448')
                    ).yield_per(10):
            target = target_row.id
            for disease_row in self.session.query(ElasticsearchLoad.id).filter(and_(
                        ElasticsearchLoad.index==Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                        ElasticsearchLoad.type==Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                        ElasticsearchLoad.active==True,
                        ElasticsearchLoad.id =='EFO_0000270')
                    ).yield_per(10):
                disease = disease_row.id
                c+=1
                evidence = self._get_evidence_for_pair(target, disease)

                if evidence or (c%1000 ==0):
                    print c, target, disease, evidence
                    # exit(0)

    def _get_evidence_for_pair(self, target, disease):
        #TODO: MANUALLY HANDLE THE QUERY
        '''
        SELECT rs.Field1,rs.Field2
            FROM (
                SELECT Field1,Field2, Rank()
                  over (Partition BY Section
                        ORDER BY RankCriteria DESC ) AS Rank
                FROM table
                ) rs WHERE Rank <= 10
        '''
        query = """SELECT * from pipeline.elasticsearch_load WHERE  index  LIKE '%s%%' AND data #>> '{target,id}' ? '%s';"""%(Config.ELASTICSEARCH_DATA_INDEX_NAME, target)
        print query
        evidences =self.session.execute(query)
        # evidences = [row.data for row in self.session.query(ElasticsearchLoad.data, ElasticsearchLoad.data[('target','id')]).filter(and_(
        #                 ElasticsearchLoad.index.like(Config.ELASTICSEARCH_DATA_INDEX_NAME+'%'),
        #                 ElasticsearchLoad.active==True,
        #                 # ElasticsearchLoad.data[('target','id')].astext == target,
        #                 # ElasticsearchLoad.data[('disease','id')].astext == disease,
        #
        #                 )
        #
        #             ).yield_per(100).limit(10)]
        evidences =  evidences.fetchall()
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
