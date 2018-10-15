# Makefile for Open Targets data pipeline
# Based on steps in https://github.com/opentargets/data_release/wiki/OT011-Data-Processing-(Backend)

# These variables can be overridden on the command-line. Note that if any of these are already specified by environment variables, those values will take precedence
ES_PREFIX ?= gp-makefile
ELASTICSEARCH_NODES ?= http://localhost:9200
LOG_LEVEL ?= DEBUG
SCHEMA_VERSION ?= 1.2.8
PERCENTAGE_TO_KEEP ?= 2

# Allow specification of additional arguments for each stage on the command-line
MRTARGET_ARGS ?=

# Internal variables
MRTARGET_CMD = python -m mrtarget.CommandLine --log-level=$(LOG_LEVEL) $(MRTARGET_ARGS) $(ES_PREFIX)
INDEX_CMD = python scripts/check_index.py --elasticsearch $(ELASTICSEARCH_NODES) --zero-fail --index
DOWNLOAD_CMD = ./scripts/download_and_trim.sh

# Main command for invoking mrtarget; note that $STAGE needs to be defined for this work.
# Also note that $@ expands to the name of the current Makefile target so saves a lot of boilerplate
TEMPLATE_CMD = $(MRTARGET_CMD) --$(STAGE) 2>&1 | tee out.$(ES_PREFIX).$(STAGE).log

# Main command for validation; see notes on variables above
VALIDATE_CMD = $(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(STAGE).json.gz 2>&1 | tee out.$(ES_PREFIX).$(STAGE).log

# First target is run by default if make is run with no arguments, so do something safe
.PHONY: dry_run
dry_run:
	$(MRTARGET_CMD) --dry-run

.PHONY: check_elasticsearch
check_elasticsearch:
	@curl -sSf $(ELASTICSEARCH_NODES) -o /dev/null

.PHONY: rea
rea: check_elasticsearch
	$(eval STAGE = $@)
	$(TEMPLATE_CMD)

.PHONY: ens
ens: check_elasticsearch
	$(eval STAGE = $@)
	$(TEMPLATE_CMD)

.PHONY: uni
uni: check_elasticsearch
	$(eval STAGE = $@)
	$(TEMPLATE_CMD)
.PHONY: unic
unic: uni

.PHONY: hpa
hpa: check_elasticsearch
	$(eval STAGE = $@)
	$(TEMPLATE_CMD)

.PHONY: mp
mp: check_elasticsearch
	$(eval STAGE = $@)
	$(TEMPLATE_CMD)

.PHONY: efo
efo: check_elasticsearch
	$(eval STAGE = $@)
	$(TEMPLATE_CMD)

.PHONY: eco
eco: check_elasticsearch
	$(eval STAGE = $@)
	$(TEMPLATE_CMD)

.PHONY: load_data
load_data: rea ens uni hpa mp efo eco

.PHONY: gene_merge
gene_merge:
	$(INDEX_CMD) $(ES_PREFIX)_reactome-data $(ES_PREFIX)_ensembl-data $(ES_PREFIX)_uniprot-data $(ES_PREFIX)_expression-data
	$(eval STAGE = gen)
	$(TEMPLATE_CMD)

.PHONY: validate_indices
validate_indices:
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data

.PHONY: validate_atlas
validate_atlas: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/atlas-14-02-2018.json.gz atlas.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = atlas)
	$(VALIDATE_CMD)

.PHONY: validate_chembl
validate_chembl: validate_indices
	$(DOWNLOAD_CMD)  https://storage.googleapis.com/ot-releases/18.08/chembl-02-08-2018.json.gz chembl.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = chembl)
	$(VALIDATE_CMD)

.PHONY: validate_cosmic
validate_cosmic: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/cosmic-24-05-2018.json.gz cosmic.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = cosmic)
	$(VALIDATE_CMD)

.PHONY: validate_europepmc
validate_europepmc: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/europepmc-27-07-2018.json.gz europepmc.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = europepmc)
	$(VALIDATE_CMD)

.PHONY: validate_eva
validate_eva: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/eva-02-08-2018.json.gz eva.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = eva)
	$(VALIDATE_CMD)

.PHONY: validate_gene2phenotype
validate_gene2phenotype: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/gene2phenotype-27-07-2018.json.gz gene2phenotype.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = gene2phenotype)
	$(VALIDATE_CMD)

.PHONY: validate_genomics_england
validate_genomics_england: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/genomics_england-23-07-2018.json.gz genomics_england.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = genomics_england)
	$(VALIDATE_CMD)

.PHONY: validate_gwas
validate_gwas: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/gwas-06-08-2018.json.gz gwas.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = gwas)
	$(VALIDATE_CMD)

.PHONY: validate_intogen
validate_intogen: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/intogen-23-07-2018.json.gz intogen.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = intogen)
	$(VALIDATE_CMD)

.PHONY: validate_phenodigm
validate_phenodigm: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/phenodigm-17-08-2018.json.gz phenodigm.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = phenodigm)
	$(VALIDATE_CMD)

.PHONY: validate_phewas_catalog
validate_phewas_catalog: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/phewas_catalog-11-09-2017.json.gz phewas_catalog.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = phewas_catalog)
	$(VALIDATE_CMD)

.PHONY: validate_progeny
validate_progeny: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/progeny-23-07-2018.json.gz progeny.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = progeny)
	$(VALIDATE_CMD)

.PHONY: validate_reactome
validate_reactome: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/reactome-19-07-2018.json.gz reactome.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = reactome)
	$(VALIDATE_CMD)

.PHONY: validate_slapenrich
validate_slapenrich: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/slapenrich-27-07-2018.json.gz slapenrich.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = slapenrich)
	$(VALIDATE_CMD)

.PHONY: validate_uniprot
validate_uniprot: validate_indices
	$(DOWNLOAD_CMD) https://storage.googleapis.com/ot-releases/18.08/uniprot-30-07-2018.json.gz uniprot.json.gz $(PERCENTAGE_TO_KEEP)
	$(eval STAGE = uniprot)
	$(VALIDATE_CMD)

.PHONY: validate_all
validate_all: validate_atlas validate_chembl validate_cosmic validate_europepmc validate_eva validate_gene2phenotype validate_genomics_england validate_gwas validate_intogen validate_phenodigm validate_phewas_catalog validate_progeny validate_reactome validate_slapenrich validate_uniprot

.PHONY: evidence_strings
evidence_strings:
	$(INDEX_CMD) $(ES_PREFIX)_validated-data-atlas $(ES_PREFIX)_validated-data-chembl $(ES_PREFIX)_validated-data-cosmic $(ES_PREFIX)_validated-data-europepmc $(ES_PREFIX)_validated-data-eva $(ES_PREFIX)_validated-data-gene2phenotype $(ES_PREFIX)_validated-data-genomics_england $(ES_PREFIX)_validated-data-gwas $(ES_PREFIX)_validated-data-intogen $(ES_PREFIX)_validated-data-phenodigm $(ES_PREFIX)_validated-data-phewas_catalog $(ES_PREFIX)_validated-data-progeny $(ES_PREFIX)_validated-data-reactome $(ES_PREFIX)_validated-data-slapenrich $(ES_PREFIX)_validated-data-uniprot
	$(eval STAGE = evs)
	$(TEMPLATE_CMD)

.PHONY: association_scores
association_scores:
	$(INDEX_CMD) $(ES_PREFIX)_evidence-data-generic
	$(eval STAGE = as)
	$(TEMPLATE_CMD)

.PHONY: association_qc
association_qc:
	$(INDEX_CMD) $(ES_PREFIX)_association-data
	$(eval STAGE = qc)
	$(TEMPLATE_CMD)

.PHONY: search_data
search_data:
	$(INDEX_CMD) $(ES_PREFIX)_association-data
	$(eval STAGE = sea)
	$(TEMPLATE_CMD)

.PHONY: relationship_data
relationship_data:
	$(INDEX_CMD) $(ES_PREFIX)_association-data
	$(eval STAGE = ddr)
	$(TEMPLATE_CMD)

.PHONY: metrics
metrics:
	$(INDEX_CMD) $(ES_PREFIX)_association-data
	$(eval STAGE = metric)
	$(TEMPLATE_CMD)

.PHONY: all
all: load_data gene_merge validate_all evidence_strings association_scores association_qc search_data relationship_data metrics

# Utility targets

.PHONY: list_indices
list_indices:
	@curl -s $(ELASTICSEARCH_NODES)/_cat/indices?v | grep -v "  \."

.PHONY: delete_indices
delete_indices:
	curl -X "DELETE" "$(ELASTICSEARCH_NODES)/$(ES_PREFIX)*"

.PHONY: clean_logs
clean_logs:
	@rm *.log

.PHONY: clean_json
clean_json:
	@rm *.json.gz

.PHONY: clean
clean: clean_logs clean_json

