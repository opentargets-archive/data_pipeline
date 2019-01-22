# Makefile for Open Targets data pipeline

#ensure pipes behave correctly in called processes
SHELL = /bin/bash
.SHELLFLAGS = -o pipefail -c

#get the directory the makefile is in
# from https://stackoverflow.com/questions/18136918
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
mkfile_dir := $(dir $(mkfile_path))

# These variables can be overridden on the command-line. 
# Note that if any of these are already specified by environment variables, 
# the existing variables will take precedence
ELASTICSEARCH_NODES ?= http://localhost:9200

LOG_PATH ?= $(mkfile_dir)/log

# Allow specification of additional arguments for each stage on the command-line
# Intended to be empty here and overriden from outside if needed
MRTARGET_ARGS ?= 

# Internal variables
#command to run mrtarget with various logging
MRTARGET_ENTRYPOINT ?= $(mkfile_dir)/scripts/entrypoint.sh
MRTARGET_CMD = $(MRTARGET_ENTRYPOINT)

# First target is run by default if make is run with no arguments, so do something safe
.PHONY: dry_run
dry_run:
	$(MRTARGET_CMD) --dry-run --release-tag dry_run

.PHONY: rea
rea: $(LOG_PATH)/out.rea.log

$(LOG_PATH)/out.rea.log : 
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --rea 2>&1 | tee $(LOG_PATH)/out.rea.log

.PHONY: ens
ens: $(LOG_PATH)/out.ens.log 

$(LOG_PATH)/out.ens.log : 
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --ens 2>&1 | tee $(LOG_PATH)/out.ens.log

.PHONY: unic
unic: uni

.PHONY: uni
uni: $(LOG_PATH)/out.uni.log

$(LOG_PATH)/out.uni.log :
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --unic 2>&1 | tee $(LOG_PATH)/out.uni.log
	sleep 60

.PHONY: hpa
hpa: $(LOG_PATH)/out.hpa.log

$(LOG_PATH)/out.hpa.log : 
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --hpa 2>&1 | tee $(LOG_PATH)/out.hpa.log
	sleep 60

.PHONY: efo
efo: $(LOG_PATH)/out.efo.log

$(LOG_PATH)/out.efo.log : 
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --efo 2>&1 | tee $(LOG_PATH)/out.efo.log
	sleep 60


.PHONY: eco
eco: $(LOG_PATH)/out.eco.log

$(LOG_PATH)/out.eco.log : 
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --eco 2>&1 | tee $(LOG_PATH)/out.eco.log
	sleep 60


.PHONY: base_gene
base_gene: $(LOG_PATH)/out.gen.log	

$(LOG_PATH)/out.gen.log : $(LOG_PATH)/out.rea.log $(LOG_PATH)/out.ens.log $(LOG_PATH)/out.uni.log
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --gen 2>&1 | tee $(LOG_PATH)/out.gen.log
	sleep 60

.PHONY: base
base: base_gene efo eco hpa

.PHONY: validate_all
validate_all : $(LOG_PATH)/out.val.log

$(LOG_PATH)/out.val.log : $(LOG_PATH)/out.gen.log $(LOG_PATH)/out.efo.log $(LOG_PATH)/out.eco.log
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --val 2>&1 | tee $(LOG_PATH)/out.val.log
	sleep 60

.PHONY: association_scores
association_scores: $(LOG_PATH)/out.as.log 

$(LOG_PATH)/out.as.log : $(LOG_PATH)/out.val.log $(LOG_PATH)/out.hpa.log
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --as 2>&1 | tee $(LOG_PATH)/out.as.log

.PHONY: association_qc
association_qc: $(LOG_PATH)/out.qc.log

$(LOG_PATH)/out.qc.log : $(LOG_PATH)/out.as.log
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --qc 2>&1 | tee $(LOG_PATH)/out.qc.log

.PHONY: search_data
search_data: $(LOG_PATH)/out.sea.log

$(LOG_PATH)/out.sea.log : $(LOG_PATH)/out.as.log
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --sea 2>&1 | tee $(LOG_PATH)/out.sea.log

.PHONY: relationship_data
relationship_data: $(LOG_PATH)/out.ddr.log

$(LOG_PATH)/out.ddr.log : $(LOG_PATH)/out.as.log
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --ddr 2>&1 | tee $(LOG_PATH)/out.ddr.log

.PHONY: metrics
metrics: $(LOG_PATH)/out.metric.log

$(LOG_PATH)/out.metric.log : $(LOG_PATH)/out.as.log
	mkdir -p $(LOG_PATH)
	$(MRTARGET_CMD) --metric 2>&1 | tee $(LOG_PATH)/out.metric.log

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
	@ELASTICSEARCH_NODES="$(ELASTICSEARCH_NODES)" \
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

.PHONY: clean
clean: clean_logs clean_indices
