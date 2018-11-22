# Makefile for Open Targets data pipeline
# Based on steps in https://github.com/opentargets/data_release/wiki/OT011-Data-Processing-(Backend)


#get the directory the makefile is in
# from https://stackoverflow.com/questions/18136918
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dir := $(dir $(mkfile_path))

# These variables can be overridden on the command-line. 
# Note that if any of these are already specified by environment variables, 
# the existing variables will take precedence
ES_PREFIX ?= makefile
ELASTICSEARCH_NODES ?= http://localhost:9200
LOG_LEVEL ?= DEBUG
SCHEMA_VERSION ?= master
NUMBER_TO_KEEP ?= 1000

LOG_PATH ?= $(mkfile_dir)/log
LOG_HTTP ?= $(LOG_PATH)/http.log

JSON_PATH ?= $(mkfile_dir)/json

QC_PATH ?= $(mkfile_dir)/qc
QC_FILE ?= $(QC_PATH)/qc.$(ES_PREFIX).tsv
QC_FILE_OLD ?= $(QC_PATH)/qc.18.08.tsv

SCRIPT_PATH ?= $(mkfile_dir)/scripts

# Allow specification of additional arguments for each stage on the command-line
# Intended to be empty here and overriden from outside if needed
MRTARGET_ARGS ?= 

# Internal variables
#command to run mrtarget with various logging
MRTARGET_CMD = python -m mrtarget.CommandLine --log-level=$(LOG_LEVEL) --qc-out=$(QC_FILE) --log-http=$(LOG_HTTP) $(MRTARGET_ARGS)


INDEX_CMD = python scripts/check_index.py --elasticsearch $(ELASTICSEARCH_NODES) --zero-fail --index

#simple command to ping elasticsearch
ELASTIC_CHECK_CMD = curl -sSf $(ELASTICSEARCH_NODES) -o /dev/null

# First target is run by default if make is run with no arguments, so do something safe

.PHONY: dry_run
dry_run:
	$(MRTARGET_CMD) --dry-run $(ES_PREFIX)

.PHONY: rea
rea: $(LOG_PATH)/out.$(ES_PREFIX).rea.log

$(LOG_PATH)/out.$(ES_PREFIX).rea.log : 
	mkdir -p $(LOG_PATH)
	$(ELASTIC_CHECK_CMD)
	$(MRTARGET_CMD) --rea $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).rea.log

.PHONY: ens
ens: $(mkfile_dir)/mrtarget/resources/ensembl-data.tsv.gz $(LOG_PATH)/out.$(ES_PREFIX).ens.log 

# Download the Ensembl data file to mrtarget/resources if necessary
$(mkfile_dir)/mrtarget/resources/ensembl-data.tsv.gz :
	$(SCRIPT_PATH)/download_ensembl.sh $(mkfile_dir)/mrtarget/resources/ensembl-data.tsv.gz

$(LOG_PATH)/out.$(ES_PREFIX).ens.log : 
	mkdir -p $(LOG_PATH)
	$(ELASTIC_CHECK_CMD)
	$(MRTARGET_CMD) --ens $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).ens.log

.PHONY: unic
unic: uni

.PHONY: uni
uni: $(LOG_PATH)/out.$(ES_PREFIX).uni.log

#handle uniprot download and local file cache
#not quite implemented yet, so were mostly ahead of the game wit this
$(JSON_PATH)/uniprot.xml.gz:
	mkdir -p $(JSON_PATH)
	curl --silent --output $(JSON_PATH)/uniprot.xml.gz "https://www.uniprot.org/uniprot/?query=reviewed%3Ayes%2BAND%2Borganism%3A9606&compress=yes&format=xml"

#$(LOG_PATH)/out.$(ES_PREFIX).uni.log :  $(JSON_PATH)/uniprot.xml.gz
$(LOG_PATH)/out.$(ES_PREFIX).uni.log :
	mkdir -p $(LOG_PATH)
	$(ELASTIC_CHECK_CMD)
	$(MRTARGET_CMD) --unic $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).uni.log
	sleep 60

.PHONY: hpa
hpa: $(LOG_PATH)/out.$(ES_PREFIX).hpa.log

$(LOG_PATH)/out.$(ES_PREFIX).hpa.log : 
	mkdir -p $(LOG_PATH)
	$(ELASTIC_CHECK_CMD)
	$(MRTARGET_CMD) --hpa $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).hpa.log
	sleep 60

.PHONY: mp
mp: $(LOG_PATH)/out.$(ES_PREFIX).mp.log

$(LOG_PATH)/out.$(ES_PREFIX).mp.log : 
	mkdir -p $(LOG_PATH)
	$(ELASTIC_CHECK_CMD)
	$(MRTARGET_CMD) --mp $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).mp.log
	sleep 60


.PHONY: efo
efo: $(LOG_PATH)/out.$(ES_PREFIX).efo.log

$(LOG_PATH)/out.$(ES_PREFIX).efo.log : 
	mkdir -p $(LOG_PATH)
	$(ELASTIC_CHECK_CMD)
	$(MRTARGET_CMD) --efo $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).efo.log
	sleep 60


.PHONY: eco
eco: $(LOG_PATH)/out.$(ES_PREFIX).eco.log

$(LOG_PATH)/out.$(ES_PREFIX).eco.log : 
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --eco $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	sleep 60


.PHONY: base_gene
base_gene: $(LOG_PATH)/out.$(ES_PREFIX).gen.log	

$(LOG_PATH)/out.$(ES_PREFIX).gen.log : $(LOG_PATH)/out.$(ES_PREFIX).rea.log $(LOG_PATH)/out.$(ES_PREFIX).ens.log $(LOG_PATH)/out.$(ES_PREFIX).uni.log $(LOG_PATH)/out.$(ES_PREFIX).hpa.log
	mkdir -p $(LOG_PATH)
	$(ELASTIC_CHECK_CMD)
	$(INDEX_CMD) $(ES_PREFIX)_reactome-data $(ES_PREFIX)_ensembl-data $(ES_PREFIX)_uniprot-data $(ES_PREFIX)_expression-data
	$(MRTARGET_CMD) --gen $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).gen.log
	sleep 60

.PHONY: base
base: base_gene mp efo eco

.PHONY: validate_atlas
validate_atlas : $(LOG_PATH)/out.$(ES_PREFIX).val.atlas.log

$(JSON_PATH)/atlas.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/atlas-27-09-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/atlas.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.atlas.log: $(JSON_PATH)/atlas.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/atlas.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.atlas.log


.PHONY: validate_chembl
validate_chembl : $(LOG_PATH)/out.$(ES_PREFIX).val.chembl.log

$(JSON_PATH)/chembl.json.gz :
	curl --silent https://storage.googleapis.com/ot-releases/18.10/chembl-27-09-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/chembl.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.chembl.log : $(JSON_PATH)/chembl.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/chembl.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.chembl.log


.PHONY: validate_cosmic
validate_cosmic : $(LOG_PATH)/out.$(ES_PREFIX).val.cosmic.log

$(JSON_PATH)/cosmic.json.gz :
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/cosmic-25-09-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/cosmic.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.cosmic.log : $(JSON_PATH)/cosmic.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/cosmic.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.cosmic.log


.PHONY: validate_europepmc
validate_europepmc : $(LOG_PATH)/out.$(ES_PREFIX).val.europepmc.log

$(JSON_PATH)/europepmc.json.gz :
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/europepmc-03-10-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/europepmc.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.europepmc.log : $(JSON_PATH)/europepmc.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/europepmc.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.europepmc.log


.PHONY: validate_eva
validate_eva : $(LOG_PATH)/out.$(ES_PREFIX).val.eva.log

$(JSON_PATH)/eva.json.gz :
	curl --silent https://storage.googleapis.com/ot-releases/18.10/eva-01-10-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/eva.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.eva.log : $(JSON_PATH)/eva.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/eva.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.eva.log



.PHONY: validate_gene2phenotype
validate_gene2phenotype : $(LOG_PATH)/out.$(ES_PREFIX).val.gene2phenotype.log

$(JSON_PATH)/gene2phenotype.json.gz :
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/gene2phenotype-19-10-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/gene2phenotype.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.gene2phenotype.log :  $(JSON_PATH)/gene2phenotype.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/gene2phenotype.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.gene2phenotype.log


.PHONY: validate_genomics_england
validate_genomics_england : $(LOG_PATH)/out.$(ES_PREFIX).val.genomics_england.log

$(JSON_PATH)/genomics_england.json.gz :
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/genomics_england-02-10-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/genomics_england.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.genomics_england.log : $(JSON_PATH)/genomics_england.json.gz
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/genomics_england.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.genomics_england.log


.PHONY: validate_gwas
validate_gwas : $(LOG_PATH)/out.$(ES_PREFIX).val.gwas.log

$(JSON_PATH)/gwas.json.gz :
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/gwas-09-10-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/gwas.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.gwas.log : $(JSON_PATH)/gwas.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/gwas.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.gwas.log


.PHONY: validate_intogen
validate_intogen : $(LOG_PATH)/out.$(ES_PREFIX).val.intogen.log 

$(JSON_PATH)/intogen.json.gz :
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/intogen-23-07-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/intogen.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.intogen.log : $(JSON_PATH)/intogen.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/intogen.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.intogen.log


.PHONY: validate_phenodigm
validate_phenodigm: $(LOG_PATH)/out.$(ES_PREFIX).val.phenodigm.log

$(JSON_PATH)/phenodigm.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/phenodigm-12-10-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/phenodigm.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.phenodigm.log : $(JSON_PATH)/phenodigm.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/phenodigm.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.phenodigm.log


.PHONY: validate_phewas_catalog
validate_phewas_catalog : $(LOG_PATH)/out.$(ES_PREFIX).val.phewas_catalog.log

$(JSON_PATH)/phewas_catalog.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/phewas_catalog-11-09-2017.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/phewas_catalog.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.phewas_catalog.log : $(JSON_PATH)/phewas_catalog.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/phewas_catalog.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.phewas_catalog.log


.PHONY: validate_progeny
validate_progeny : $(LOG_PATH)/out.$(ES_PREFIX).val.progeny.log

$(JSON_PATH)/progeny.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/progeny-23-07-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/progeny.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.progeny.log : $(JSON_PATH)/progeny.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/progeny.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.progeny.log


.PHONY: validate_reactome
validate_reactome : $(LOG_PATH)/out.$(ES_PREFIX).val.reactome.log 

$(JSON_PATH)/reactome.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/reactome-03-09-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/reactome.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.reactome.log : $(JSON_PATH)/reactome.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/reactome.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.reactome.log


.PHONY: validate_slapenrich
validate_slapenrich : $(LOG_PATH)/out.$(ES_PREFIX).val.slapenrich.log

$(JSON_PATH)/slapenrich.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/slapenrich-27-07-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/slapenrich.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.slapenrich.log : $(JSON_PATH)/slapenrich.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/slapenrich.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.slapenrich.log


.PHONY: validate_uniprot
validate_uniprot : $(LOG_PATH)/out.$(ES_PREFIX).val.uniprot.log

$(JSON_PATH)/uniprot.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.10/uniprot-26-09-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/uniprot.json.gz

$(LOG_PATH)/out.$(ES_PREFIX).val.uniprot.log : $(JSON_PATH)/uniprot.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).mp.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data $(ES_PREFIX)_mp-data
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val --input-file $(JSON_PATH)/uniprot.json.gz $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.uniprot.log

.PHONY: validate_all
validate_all : validate_uniprot validate_slapenrich validate_reactome validate_progeny validate_phewas_catalog validate_phenodigm validate_intogen validate_gwas validate_genomics_england validate_gene2phenotype validate_eva validate_europepmc validate_cosmic validate_chembl validate_atlas

.PHONY: evidence_strings
evidence_strings: $(LOG_PATH)/out.$(ES_PREFIX).evs.log

$(LOG_PATH)/out.$(ES_PREFIX).evs.log : $(LOG_PATH)/out.$(ES_PREFIX).val.atlas.log $(LOG_PATH)/out.$(ES_PREFIX).val.chembl.log $(LOG_PATH)/out.$(ES_PREFIX).val.cosmic.log $(LOG_PATH)/out.$(ES_PREFIX).val.europepmc.log $(LOG_PATH)/out.$(ES_PREFIX).val.eva.log $(LOG_PATH)/out.$(ES_PREFIX).val.gene2phenotype.log $(LOG_PATH)/out.$(ES_PREFIX).val.genomics_england.log $(LOG_PATH)/out.$(ES_PREFIX).val.gwas.log $(LOG_PATH)/out.$(ES_PREFIX).val.intogen.log $(LOG_PATH)/out.$(ES_PREFIX).val.phenodigm.log $(LOG_PATH)/out.$(ES_PREFIX).val.phewas_catalog.log $(LOG_PATH)/out.$(ES_PREFIX).val.progeny.log $(LOG_PATH)/out.$(ES_PREFIX).val.reactome.log $(LOG_PATH)/out.$(ES_PREFIX).val.slapenrich.log $(LOG_PATH)/out.$(ES_PREFIX).val.uniprot.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_validated-data-atlas $(ES_PREFIX)_validated-data-chembl $(ES_PREFIX)_validated-data-cosmic $(ES_PREFIX)_validated-data-europepmc $(ES_PREFIX)_validated-data-eva $(ES_PREFIX)_validated-data-gene2phenotype $(ES_PREFIX)_validated-data-genomics_england $(ES_PREFIX)_validated-data-gwas $(ES_PREFIX)_validated-data-intogen $(ES_PREFIX)_validated-data-phenodigm $(ES_PREFIX)_validated-data-phewas_catalog $(ES_PREFIX)_validated-data-progeny $(ES_PREFIX)_validated-data-reactome $(ES_PREFIX)_validated-data-slapenrich $(ES_PREFIX)_validated-data-uniprot
	$(MRTARGET_CMD) --evs $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).evs.log

.PHONY: association_scores
association_scores: $(LOG_PATH)/out.$(ES_PREFIX).as.log

$(LOG_PATH)/out.$(ES_PREFIX).as.log : $(LOG_PATH)/out.$(ES_PREFIX).evs.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_evidence-data-generic
	$(MRTARGET_CMD) --as $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).as.log

.PHONY: association_qc
association_qc: $(LOG_PATH)/out.$(ES_PREFIX).qc.log

$(LOG_PATH)/out.$(ES_PREFIX).qc.log : $(LOG_PATH)/out.$(ES_PREFIX).as.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_association-data
	$(MRTARGET_CMD) --qc $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).qc.log

.PHONY: search_data
search_data: $(LOG_PATH)/out.$(ES_PREFIX).sea.log

$(LOG_PATH)/out.$(ES_PREFIX).sea.log : $(LOG_PATH)/out.$(ES_PREFIX).as.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_association-data
	$(MRTARGET_CMD) --sea $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).sea.log

.PHONY: relationship_data
relationship_data: $(LOG_PATH)/out.$(ES_PREFIX).ddr.log

$(LOG_PATH)/out.$(ES_PREFIX).ddr.log : $(LOG_PATH)/out.$(ES_PREFIX).as.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_association-data
	$(MRTARGET_CMD) --ddr $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).ddr.log

.PHONY: metrics
metrics: $(LOG_PATH)/out.$(ES_PREFIX).metric.log

$(LOG_PATH)/out.$(ES_PREFIX).metric.log : $(LOG_PATH)/out.$(ES_PREFIX).as.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_association-data
	$(MRTARGET_CMD) --metric $(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).metric.log

.PHONY: all
all: metrics relationship_data search_data association_qc

# Utility targets
# thanks to https://stackoverflow.com/a/15058900
.PHONY: no_targets__ list
no_targets__:
list:
	@sh -c "$(MAKE) -p no_targets__ | awk -F':' '/^[a-zA-Z0-9][^\$$#\/\\t=]*:([^=]|$$)/ {split(\$$1,A,/ /);for(i in A)print A[i]}' | grep -v '__\$$' | sort"


.PHONY: no_targets__ shell
no_targets__:
shell:
	@ES_PREFIX="$(ES_PREFIX)" \
	    ELASTICSEARCH_NODES="$(ELASTICSEARCH_NODES)" \
	    LOG_LEVEL="$(LOG_LEVEL)" \
	    SCHEMA_VERSION="$(SCHEMA_VERSION)" \
	    NUMBER_TO_KEEP="$(NUMBER_TO_KEEP)" \
	    MRTARGET_CMD="$(MRTARGET_CMD)" \
	    bash

.PHONY: list_indices
list_indices:
	@curl -s $(ELASTICSEARCH_NODES)/_cat/indices?v | grep -v "  \."

.PHONY: clean_indices
clean_indices:
	curl -X "DELETE" "$(ELASTICSEARCH_NODES)/$(ES_PREFIX)*"

.PHONY: clean_logs
clean_logs:
	@rm -f $(LOG_PATH)/*.log

.PHONY: clean_json
clean_json:
	@rm -f $(JSON_PATH)/*.json.gz

.PHONY: clean_qc
clean_qc:
	@rm -f $(QC_PATH)/*.tsv

.PHONY: clean
clean: clean_logs clean_json clean_qc clean_indices

