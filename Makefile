# Makefile for Open Targets data pipeline
# Based on steps in https://github.com/opentargets/data_release/wiki/OT011-Data-Processing-(Backend)

#ensure pipes behave correctly
SHELL = /bin/bash
.SHELLFLAGS = -o pipefail -c

#get the directory the makefile is in
# from https://stackoverflow.com/questions/18136918
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dir := $(dir $(mkfile_path))

# These variables can be overridden on the command-line. 
# Note that if any of these are already specified by environment variables, 
# the existing variables will take precedence
ES_PREFIX ?= test
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
MRTARGET_ENTRYPOINT ?= $(mkfile_dir)/scripts/entrypoint.sh
MRTARGET_CMD = $(MRTARGET_ENTRYPOINT) --log-level=$(LOG_LEVEL) --qc-out=$(QC_FILE) --log-http=$(LOG_HTTP) $(MRTARGET_ARGS)


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
ens: $(LOG_PATH)/out.$(ES_PREFIX).ens.log 

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
base: base_gene efo eco

$(JSON_PATH)/atlas.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/atlas-2018-11-20.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/atlas.json.gz

$(JSON_PATH)/chembl.json.gz :
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/chembl-28-11-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/chembl.json.gz

$(JSON_PATH)/cosmic.json.gz :
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/cosmic-27-11-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/cosmic.json.gz

$(JSON_PATH)/europepmc.json.gz :
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/europepmc-29-11-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/europepmc.json.gz

$(JSON_PATH)/eva.json.gz :
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/eva-01-10-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/eva.json.gz

$(JSON_PATH)/gene2phenotype.json.gz :
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/gene2phenotype-29-11-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/gene2phenotype.json.gz

$(JSON_PATH)/genomics_england.json.gz :
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/genomics_england-02-10-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/genomics_england.json.gz

$(JSON_PATH)/gwas.json.gz :
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/gwas-28-11-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/gwas.json.gz

$(JSON_PATH)/intogen.json.gz :
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/intogen-23-07-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/intogen.json.gz

$(JSON_PATH)/phenodigm.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/phenodigm-12-10-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/phenodigm.json.gz

$(JSON_PATH)/phewas_catalog.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/phewas_catalog-28-11-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/phewas_catalog.json.gz

$(JSON_PATH)/progeny.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/progeny-23-07-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/progeny.json.gz

$(JSON_PATH)/reactome.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/reactome-03-09-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/reactome.json.gz

$(JSON_PATH)/slapenrich.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/slapenrich-29-11-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/slapenrich.json.gz

$(JSON_PATH)/sysbio.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/sysbio-28-11-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/sysbio.json.gz

$(JSON_PATH)/uniprot.json.gz:
	mkdir -p $(JSON_PATH)
	curl --silent https://storage.googleapis.com/ot-releases/18.12/evidences/uniprot-15-11-2018.json.gz | gunzip -c -- | shuf -n $(NUMBER_TO_KEEP) | gzip > $(JSON_PATH)/uniprot.json.gz

.PHONY: validate_all
validate_all : $(LOG_PATH)/out.$(ES_PREFIX).val.log

$(LOG_PATH)/out.$(ES_PREFIX).val.log : $(JSON_PATH)/atlas.json.gz $(JSON_PATH)/chembl.json.gz $(JSON_PATH)/cosmic.json.gz $(JSON_PATH)/europepmc.json.gz $(JSON_PATH)/eva.json.gz $(JSON_PATH)/gene2phenotype.json.gz $(JSON_PATH)/genomics_england.json.gz $(JSON_PATH)/gwas.json.gz $(JSON_PATH)/intogen.json.gz $(JSON_PATH)/phenodigm.json.gz $(JSON_PATH)/phewas_catalog.json.gz $(JSON_PATH)/progeny.json.gz $(JSON_PATH)/reactome.json.gz $(JSON_PATH)/slapenrich.json.gz $(JSON_PATH)/uniprot.json.gz $(LOG_PATH)/out.$(ES_PREFIX).gen.log $(LOG_PATH)/out.$(ES_PREFIX).efo.log $(LOG_PATH)/out.$(ES_PREFIX).eco.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_gene-data $(ES_PREFIX)_efo-data $(ES_PREFIX)_eco-data 
	$(MRTARGET_CMD) --schema-version $(SCHEMA_VERSION) --val \
		--input-file $(JSON_PATH)/atlas.json.gz \
		--input-file $(JSON_PATH)/chembl.json.gz \
		--input-file $(JSON_PATH)/cosmic.json.gz \
		--input-file $(JSON_PATH)/europepmc.json.gz \
		--input-file $(JSON_PATH)/eva.json.gz \
		--input-file $(JSON_PATH)/gene2phenotype.json.gz \
		--input-file $(JSON_PATH)/genomics_england.json.gz \
		--input-file $(JSON_PATH)/gwas.json.gz \
		--input-file $(JSON_PATH)/intogen.json.gz \
		--input-file $(JSON_PATH)/phenodigm.json.gz \
		--input-file $(JSON_PATH)/phewas_catalog.json.gz \
		--input-file $(JSON_PATH)/progeny.json.gz \
		--input-file $(JSON_PATH)/reactome.json.gz \
		--input-file $(JSON_PATH)/slapenrich.json.gz \
		--input-file $(JSON_PATH)/uniprot.json.gz \
		$(ES_PREFIX) 2>&1 | tee $(LOG_PATH)/out.$(ES_PREFIX).val.log
	sleep 60

.PHONY: association_scores
association_scores: $(LOG_PATH)/out.$(ES_PREFIX).as.log

$(LOG_PATH)/out.$(ES_PREFIX).as.log : $(LOG_PATH)/out.$(ES_PREFIX).val.log
	mkdir -p $(LOG_PATH)
	$(INDEX_CMD) $(ES_PREFIX)_evidence-data
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
