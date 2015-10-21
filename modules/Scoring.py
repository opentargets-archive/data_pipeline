import logging
import pprint
from sqlalchemy import and_, func
from sqlalchemy.orm import joinedload, subqueryload
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
            self.datasources[ds]=0.0
            self.datatypes[dt]=0.0

class AssociationScoreSet(JSONSerializable):

    def __init__(self, target, disease):
        self.target = target
        self.disease = disease
        for method_key, method in ScoringMethods.__dict__.items():
            if not method_key.startswith('_'):
                self.set_method(method,AssociationScore())
    def get_method(self, method):
        if method not in ScoringMethods.__dict__.values():
            raise AttributeError("method need to be a valid ScoringMethods")
        return self.__dict__[method]

    def set_method(self, method, score):
        if method not in ScoringMethods.__dict__.values():
            raise AttributeError("method need to be a valid ScoringMethods")
        if not isinstance(score, AssociationScore):
            raise AttributeError("score need to be an instance of AssociationScore")
        self.__dict__[method] = score



class Scorer():

    def __init__(self):
        pass

    def score(self,target, disease, evidence, method  = None):

        score = AssociationScoreSet(target,disease)

        if (method == ScoringMethods.HARMONIC_SUM) or (method is None):
            self._harmonic_sum(evidence, score)
        if (method == ScoringMethods.SUM) or (method is None):
            self._sum(evidence, score)
        if (method == ScoringMethods.MAX) or (method is None):
            self._max(evidence, score)

        return score

    def _harmonic_sum(self, evidence, score):
        return score

    def _sum(self, evidence, score):
        sum_score = score.get_method(ScoringMethods.SUM)
        for e in evidence:
            e = Evidence(e).evidence
            e_score = e['scores']['association_score']
            sum_score.overall+=e_score
            sum_score.datatypes[e['type']]+=e_score
            sum_score.datasources[e['sourceID']]+=e_score

        return

    def _max(self, evidence, score):
        max_score = score.get_method(ScoringMethods.MAX)
        for e in evidence:
            e = Evidence(e).evidence
            e_score = e['scores']['association_score']
            if e_score > max_score.datasources[e['sourceID']]:
                max_score.datasources[e['sourceID']] = e_score
                if e_score > max_score.datatypes[e['type']]:
                    max_score.datatypes[e['type']]=e_score
                    if e_score > max_score.overall:
                        max_score.overall=e_score

        return




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

        rows_deleted = self.session.query(
                TargetToDiseaseAssociationScoreMap).delete(synchronize_session='fetch')

        if rows_deleted:
            logging.info('deleted %i rows from elasticsearch_load' % rows_deleted)
        c, i = 0, 0
        for row in self.session.query(ElasticsearchLoad.data).filter(and_(
                        ElasticsearchLoad.index.like(Config.ELASTICSEARCH_DATA_INDEX_NAME+'%'),
                        ElasticsearchLoad.active==True,
                        )
                    ).yield_per(100):
            c+=1
            evidence = Evidence(row.data).evidence
            for efo in evidence['_private']['efo_codes']:
                i+=1
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
            if i % 10000 == 0:
                logging.info("%i rows inserted to score-data table, %i evidence strings analysed" %(i, c))

        self.session.commit()

class ScoreStorer():
    def __init__(self, adapter, chunk_size=100):

        self.adapter=adapter
        self.session=adapter.session
        self.chunk_size = chunk_size
        self.cache = []
        self.counter = 0

    def put(self, score):

        self.cache.append(score)
        self.counter +=1
        if (len(self.cache) % self.chunk_size) == 0:
            self.flush()


    def flush(self):

        #TODO: store in postgres
        for i,data in enumerate(self.cache):
            logging.debug(data.to_json())

        if (self.counter % self.chunk_size) == 0:
            logging.info("%i precalculated scores inserted in elasticsearch_load table" %(self.counter))

        self.session.flush()
        self.cache = []


    def close(self):

        self.flush()
        self.session.commit()


    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        self.close()

class ScoringProcess():

    def __init__(self,
                 adapter):
        self.adapter=adapter
        self.session=adapter.session
        self.scorer = Scorer()

    def process_all(self):
        self.score_target_disease_pairs()

    def score_target_disease_pairs(self):
        with ScoreStorer(self.adapter) as storer:
            target_total = self.session.query(ElasticsearchLoad.id).filter(and_(
                            ElasticsearchLoad.index==Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                            ElasticsearchLoad.type==Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                            ElasticsearchLoad.active==True,
                            )
                        ).count()
            disease_total = self.session.query(ElasticsearchLoad.id).filter(and_(
                            ElasticsearchLoad.index==Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                            ElasticsearchLoad.type==Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                            ElasticsearchLoad.active==True,
                            )
                        ).count()
            estimated_total = target_total*disease_total
            logging.info("%i targets available | %i diseases available | %iM estimated combination to precalculate"%(target_total,
                                                                                                                    disease_total,
                                                                                                                    estimated_total/1e6))

            c=0.
            combination_with_data = 0.
            for target_row in self.session.query(ElasticsearchLoad.id).filter(and_(
                            ElasticsearchLoad.index==Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                            ElasticsearchLoad.type==Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                            ElasticsearchLoad.active==True,
                            # ElasticsearchLoad.id == 'ENSG00000113448',
                            )
                        ).yield_per(10):
                target = target_row.id
                for disease_row in self.session.query(ElasticsearchLoad.id).filter(and_(
                            ElasticsearchLoad.index==Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                            ElasticsearchLoad.type==Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                            ElasticsearchLoad.active==True,
                            # ElasticsearchLoad.id =='EFO_0000270',
                            )
                        ).yield_per(10):
                    disease = disease_row.id
                    c+=1
                    evidence = self._get_evidence_for_pair(target, disease)
                    if evidence:
                        score = self.scorer.score(target, disease, evidence)
                        combination_with_data +=1
                        storer.put(score)
                        print c,round(c/estimated_total,2), target, disease, len(evidence)
                    if c%(estimated_total/1000) ==0:
                        logging.info('%1.2f%% combinations computed, %i with data'%(round(c/estimated_total), combination_with_data))
                        # print c,round(estimated_total/c,2) target, disease, len(evidence)
                        # exit(0)
            logging.info('%i%% combinations computed, %i with data, sparse ratio: %1.2f%%'%(int(round(c/estimated_total)),
                                                                                            combination_with_data,
                                                                                            (estimated_total-combination_with_data)/estimated_total))


    def _get_evidence_for_pair(self, target, disease):
        '''
        SELECT rs.Field1,rs.Field2
            FROM (
                SELECT Field1,Field2, Rank()
                  over (Partition BY Section
                        ORDER BY RankCriteria DESC ) AS Rank
                FROM table
                ) rs WHERE Rank <= 10
        '''
        # evidences =self.session.execute(query)
        evidence_ids_subquery = self.session.query(
                                                TargetToDiseaseAssociationScoreMap.evidence_id
                                                   )\
                                            .filter(
                                                and_(
                                                    TargetToDiseaseAssociationScoreMap.target_id == target,
                                                    TargetToDiseaseAssociationScoreMap.disease_id == disease,
                                                    )
                                                )\
                                            .order_by(
                                                TargetToDiseaseAssociationScoreMap.association_score.desc()
                                                )\
                                            .subquery()

        evidences = [row.data
                        for row in self.session.query(ElasticsearchLoad.data).filter(ElasticsearchLoad.id.in_(evidence_ids_subquery))
                     ]
        return evidences




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
