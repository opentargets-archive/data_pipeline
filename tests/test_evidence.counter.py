import json
import unittest

from mrtarget.modules.EvidenceString import  EvidenceGlobalCounter


literature_evidence ='''
{
    "access_level": "public", 
    "disease": {
        "efo_info": {
            "efo_id": "http://www.ebi.ac.uk/efo/EFO_0000311", 
            "label": "cancer", 
            "path": [
                [
                    "EFO_0000616", 
                    "EFO_0000311"
                ]
            ], 
            "therapeutic_area": {
                "codes": [
                    "EFO_0000616"
                ], 
                "labels": [
                    "neoplasm"
                ]
            }
        }, 
        "id": "EFO_0000311", 
        "name": "cancer"
    }, 
    "evidence": {
        "date_asserted": "2017-06-01T00:00:00Z", 
        "evidence_codes": [
            "literature_mining", 
            "ECO_0000213"
        ], 
        "evidence_codes_info": [
            [
                {
                    "eco_id": "literature_mining", 
                    "label": "Literature mining"
                }
            ], 
            [
                {
                    "eco_id": "ECO_0000213", 
                    "label": "combinatorial evidence used in automatic assertion"
                }
            ]
        ], 
        "is_associated": true, 
        "literature_ref": {
            "lit_id": "http://europepmc.org/abstract/MED/28479250", 
            "mined_sentences": [
                {
                    "d_end": 28, 
                    "d_start": 23, 
                    "section": "abstract", 
                    "t_end": 59, 
                    "t_start": 50, 
                    "text": "Our results reveal the cancer-promoting effect of FBXL19-AS1, acting as a molecular sponge in negatively modulating miR-203, which might provide a new insight for understanding of CRC development."
                }
            ]
        }, 
        "provenance_type": {
            "database": {
                "id": "EuropePMC", 
                "version": "2017-08-23"
            }
        }, 
        "resource_score": {
            "method": {
                "description": "Custom text-mining method for target-disease association"
            }, 
            "type": "summed_total", 
            "value": 8.4
        }, 
        "unique_experiment_reference": "http://europepmc.org/abstract/MED/28479250"
    }, 
    "id": "d924b37a5c63319879d6e30419e0534f", 
    "literature": {
        "references": [
            {
                "lit_id": "http://europepmc.org/abstract/MED/28479250"
            }
        ]
    }, 
    "private": {
        "datasource": "europepmc", 
        "datatype": "literature", 
        "eco_codes": [
            "literature_mining", 
            "ECO_0000213"
        ], 
        "efo_codes": [
            "EFO_0000616", 
            "EFO_0000311"
        ], 
        "facets": {}
    }, 
    "scores": {
        "association_score": 0.084
    }, 
    "sourceID": "europepmc", 
    "target": {
        "activity": "up_or_down", 
        "gene_info": {
            "geneid": "ENSG00000260852", 
            "name": "FBXL19 antisense RNA 1 (head to head)", 
            "symbol": "FBXL19-AS1"
        }, 
        "id": "ENSG00000260852", 
        "target_type": "protein_evidence"
    }, 
    "type": "literature", 
    "unique_association_fields": {
        "disease_uri": "http://www.ebi.ac.uk/efo/EFO_0000311", 
        "publicationIDs": "http://europepmc.org/abstract/MED/28479250", 
        "target": "http://identifiers.org/uniprot/Q494R0"
    }, 
    "validated_against_schema_version": "1.2.7"
}
'''
rna_evidence='''
{
    "access_level": "public", 
    "disease": {
        "biosample": {
            "id": "http://purl.obolibrary.org/obo/UBERON_0001090", 
            "name": "synovial fluid"
        }, 
        "efo_info": {
            "efo_id": "http://www.ebi.ac.uk/efo/EFO_0003778", 
            "label": "psoriatic arthritis", 
            "path": [
                [
                    "EFO_0000540", 
                    "EFO_0003778"
                ]
            ], 
            "therapeutic_area": {
                "codes": [
                    "EFO_0000540"
                ], 
                "labels": [
                    "immune system disease"
                ]
            }
        }, 
        "id": "EFO_0003778"
    }, 
    "evidence": {
        "comparison_name": "'psoriatic arthritis' vs 'healthy' in 'synovial fluid'", 
        "confidence_level": "medium", 
        "date_asserted": "2016-01-08T13:51:40Z", 
        "evidence_codes": [
            "ECO_0000356"
        ], 
        "evidence_codes_info": [
            [
                {
                    "eco_id": "ECO_0000356", 
                    "label": "differential gene expression evidence from microarray experiment"
                }
            ]
        ], 
        "experiment_overview": "GENE EXPRESSION ANALYSIS  OF SYNOVIAL BIOPSIES AND PERIPHERAL BLOOD SAMPLES FROM PSORIATIC ARTHRITIS", 
        "is_associated": true, 
        "log2_fold_change": {
            "percentile_rank": 93, 
            "value": 1.1
        }, 
        "organism_part": "http://purl.obolibrary.org/obo/UBERON_0001090", 
        "provenance_type": {
            "database": {
                "id": "Expression_Atlas", 
                "version": "dev"
            }
        }, 
        "reference_replicates_n": 5, 
        "reference_sample": "synovial fluid; healthy", 
        "resource_score": {
            "method": {
                "description": "Moderated <i>t</i>-statistics computed with ..."
            }, 
            "type": "pvalue", 
            "value": 0.00331
        }, 
        "test_replicates_n": 5, 
        "test_sample": "synovial fluid; psoriatic arthritis", 
        "unique_experiment_reference": "STUDYID_E-MTAB-3201", 
        "urls": [
            {
                "nice_name": "ArrayExpress Experiment overview", 
                "url": "http://identifiers.org/arrayexpress/E-MTAB-3201"
            }, 
            {
                "nice_name": "Gene expression in Expression Atlas", 
                "url": "http://www.ebi.ac.uk/gxa/experiments/E-MTAB-3201?geneQuery=ENSG00000166503"
            }, 
            {
                "nice_name": "Baseline gene expression in Expression Atlas", 
                "url": "http://www.ebi.ac.uk/gxa/genes/ENSG00000166503"
            }
        ]
    }, 
    "id": "f7f0b6ce4033e42a3e94281a131f79c0", 
    "private": {
        "datasource": "expression_atlas", 
        "datatype": "rna_expression", 
        "eco_codes": [
            "ECO_0000356"
        ], 
        "efo_codes": [
            "EFO_0000540", 
            "EFO_0003778"
        ], 
        "facets": {}
    }, 
    "scores": {
        "association_score": 0.0253721596236744
    }, 
    "sourceID": "expression_atlas", 
    "target": {
        "activity": "unknown", 
        "gene_info": {
            "geneid": "ENSG00000166503", 
            "name": "HDGF like 3", 
            "symbol": "HDGFL3"
        }, 
        "id": "ENSG00000166503", 
        "target_type": "transcript_evidence"
    }, 
    "type": "rna_expression", 
    "unique_association_fields": {
        "comparison_name": "'psoriatic arthritis' vs 'healthy' in 'synovial fluid'", 
        "geneID": "http://identifiers.org/ensembl/ENSG00000166503", 
        "study_id": "http://identifiers.org/gxa.expt/E-MTAB-3201"
    }, 
    "validated_against_schema_version": "1.2.7"
}
'''

class EvidenceGlobalCounterTestCase(unittest.TestCase):

    def setUp(self):
        self.literature_test_data = json.loads(literature_evidence)
        self.rna_test_data = json.loads(rna_evidence)
        self.global_counts = EvidenceGlobalCounter()
        self.global_counts.digest(self.rna_test_data)
        self.global_counts.digest(self.rna_test_data)
        self.global_counts.digest(self.literature_test_data)
        self.global_counts.compress()

    def test_digest(self):
        self.assertEqual(EvidenceGlobalCounter.get_target(self.literature_test_data), 'ENSG00000260852')
        self.assertEqual(EvidenceGlobalCounter.get_disease(self.literature_test_data), 'EFO_0000311')
        self.assertEqual(EvidenceGlobalCounter.get_literature(self.literature_test_data), ['28479250'])

        self.assertEqual(EvidenceGlobalCounter.get_target(self.rna_test_data), 'ENSG00000166503')
        self.assertEqual(EvidenceGlobalCounter.get_disease(self.rna_test_data), 'EFO_0003778')
        self.assertEqual(EvidenceGlobalCounter.get_experiment(self.rna_test_data), 'STUDYID_E-MTAB-3201')

    def test_count(self):
        global_counts = EvidenceGlobalCounter()
        global_counts.digest(self.rna_test_data)
        global_counts.digest(self.rna_test_data)
        global_counts.digest(self.literature_test_data)
        self.assertEquals(global_counts.total['target']['ENSG00000260852'],1)
        self.assertEquals(global_counts.total['target']['ENSG00000166503'],2)
        self.assertEquals(global_counts.total['disease']['EFO_0003778'],2)
        self.assertEquals(global_counts.total['disease']['EFO_0000311'],1)
        self.assertEquals(global_counts.total['total'], 3)
        self.assertEquals(global_counts.literature['28479250']['target']['ENSG00000260852'], 1)
        self.assertEquals(global_counts.literature['28479250']['disease']['EFO_0000311'], 1)
        self.assertEquals(global_counts.literature['28479250']['total'], 1)
        self.assertEquals(global_counts.experiment['STUDYID_E-MTAB-3201']['target']['ENSG00000166503'], 2)
        self.assertEquals(global_counts.experiment['STUDYID_E-MTAB-3201']['disease']['EFO_0003778'], 2)
        self.assertEquals(global_counts.experiment['STUDYID_E-MTAB-3201']['total'], 2)

    def test_retrieval(self):
        self.assertEquals((1,1), self.global_counts.get_target_and_disease_uniques_for_literature('28479250'))
        self.assertEquals((0, 0), self.global_counts.get_target_and_disease_uniques_for_literature('not_existing'))
        self.assertEquals((1, 1), self.global_counts.get_target_and_disease_uniques_for_experiment('STUDYID_E-MTAB-3201'))
        self.assertEquals((0, 0), self.global_counts.get_target_and_disease_uniques_for_experiment('not_existing'))




if __name__ == '__main__':
    unittest.main()
