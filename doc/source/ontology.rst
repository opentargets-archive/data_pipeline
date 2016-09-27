.. _ontology:

Ontology ETL Processes
**********************

Here, we describe the different ontology extraction, transformation and loading processes in the data
pipeline.

Ontology sources
================


Evidence and Conclusion Ontology
--------------------------------

ECO is the main ontology to describe and organise the knowledge around evidence in Open Targets.
Each type of evidence from individual datasources are tagged with one or more evidence codes.
When the project was originally developed, extra evidence IRI were created to cope with the fact that some evidence were
specific to Open Targets. For instance, text mining evidence would be represented as
'http://www.targetvalidation.org/evidence/literature_mining'.
And the root of evidence would be represented as 'http://www.targetvalidation.org/disease/cttv_evidence'.
Because these terms (and others) are not standard, we have decided to replace them with existing ECO terms whenever
possible to avoid extra manual work and extra maintenance of an evidence ontology.

For information, here is the mapping of bespoke CTTV terms to existing ECO terms:
* http://www.targetvalidation.org/disease/cttv_evidence, CTTV evidence: http://purl.obolibrary.org/obo/ECO_0000000, evidence
* http://identifiers.org/eco/target_drug, target_drug: http://purl.obolibrary.org/obo/ECO_0000360, biological target-disease association via drug
* http://identifiers.org/eco/drug_disease, drug_disease: http://purl.obolibrary.org/obo/ECO_0000360, biological target-disease association via drug
* http://identifiers.org/eco/clinvar_gene_assignments, ClinVAR SNP-gene pipeline: not used
* http://identifiers.org/eco/cttv_mapping_pipeline, CTTV-custom annotation pipeline: http://purl.obolibrary.org/obo/ECO_0000246, computational combinatorial evidence used in automatic assertion
* http://identifiers.org/eco/GWAS_fine_mapping, Fine-mapping study evidence: not used

We kept:
* http://www.targetvalidation.org/evidence/genomics_evidence, genomics evidence: as a child of evidence

Genomics evidence will encompass all the

Sequence Types and Features Ontology
------------------------------------

The Sequence Ontology (SO) describes the parts of the genome and how they relate to each other in topological space and
other dimensions such as regulatory space. It includes terms and relations to describe not only gene models but other
genomic phenomena such as transposons and repeats. We are primarily interested in using SO because it also describes
variations and the consequences of variation.

We use SO terms to determine the functional consequence of the variants that are sent by the data providers (GWAS
Catalog, EVA, UniProt). Additionally, the consequence is used to assign a score to an evidence. See mapping table in
data_pipeline/resources/eco_scores (which really should be called functional consequence scores).

We had a few terms not represented in SO but we decided to map them to existing SO classes.

For end-users, the documentation is located in https://www.targetvalidation.org/variants


Nearest gene 5' end
+++++++++++++++++++

http://targetvalidation.org/sequence/regulatory_nearest_gene_five_prime_end, Nearest regulatory gene from 5' end

This term refers to the outcome of the current SNP to effector nearest gene method which assigns the nearest 5' end of
a gene. Note that this is not necessarily upstream (only close by the 5' end).

The current method is based on using the Bio::EnsEMBL::DBSQL::BaseFeatureAdaptor::fetch_all_by_outward_search method.
This method, given a variation feature (a SNP), a range (1Kb), a max range (50,000), a limit of features to return (40)
and a direction (FIVE_PRIME), will search for features in both directions (upstream/downstream) within the range given,
until it satisfies the limit or hits the max range. The -FIVE_PRIME option allow searching for specific ends of nearby
features and discount any features with their other end in the overlap region. Then only the protein coding gene
features are kept and the closest gene is kept.

This term will be replaced by: <SPECS>
* http://purl.obolibrary.org/obo/SO_0001631 upstream_gene_variant
* http://purl.obolibrary.org/obo/SO_0001632 downstream_gene_variant

Nearest regulatory gene from 5' end
+++++++++++++++++++++++++++++++++++

http://targetvalidation.org/sequence/regulatory_nearest_gene_five_prime_end, Nearest regulatory gene from 5' end.

It is currently not used
http://targetvalidation.org/sequence/nearest_gene_five_prime_end,	Nearest gene counting from 5' end:

http://purl.obolibrary.org/obo/SO_0001631 upstream_gene_variant

Is a combination of a "regulatory region variant" and nearest gene 5' end.

* http://targetvalidation.org/sequence/regulatory_nearest_gene_five_prime_end, Nearest regulatory gene from 5' end: need to check if it is ever used* http://targetvalidation.org/sequence/nearest_gene_five_prime_end,	Nearest gene counting from 5' end: http://purl.obolibrary.org/obo/SO_0001631 upstream_gene_variant

Disease phenotypes
+++++++++++++++++