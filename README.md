
The code in this repository is used to process different data files that provide evidence for the target-disease 
associations in the [Open Targets Platform](https://www.targetvalidation.org). Documentation on how to use the 
platform can be found [here](https://docs.targetvalidation.org) and the evidence and association data dumps can be 
 found [here](https://www.targetvalidation.org/downloads/data). Please contact support [at] targetvalidation.org for 
 feedback.
 
 This page contains the following information:
- Overview of the pipeline
- Running the pipeline locally
- Running the pipeline using Docker
- (Installation Instructions)
- Using the Makefile
- Using the Makefile within Docker
- (Environment variables and how to use them)
- Contributing
- Copyright


[![Build Status](https://travis-ci.com/opentargets/data_pipeline.svg?branch=master)](https://travis-ci.com/opentargets/data_pipeline)
[quay.io](https://quay.io/repository/opentargets/mrtarget)

---

## Overview of the pipeline

TO DO: Add overview diagram here

#### 1. Loading the Base Data
- All steps in this section are independent of each other and can be run in parallel and in any order as needed.
- Each step will download, process and create indices in Elasticsearch for the different datasets (see diagram above).
#### 2. Gene merging
- Pre-requisites: Step 1 needs to be completed because Elasticsearch indices created for Expression Atlas, Reactome, 
Uniprot and Ensembl are needed.
- This step creates the index `${DATA_RELEASE_VERSION}_gene-data`.
#### 3. Evidence Data Validation
- Pre-requisites: Steps 1 and 2 need to be completed because Elasticsearch indices for efo, eco and gene-data indices 
are needed. Any JSON schema changes that are required need to be finalised.
- This step will create a new index per datasource e.g. `${DATA_RELEASE_VERSION}_validated-data-reactome`.
- Data sources to include are specified in a 
[config file.](https://github.com/opentargets/data_pipeline/blob/master/mrtarget/resources/evidences_sources.txt)
This file can be edited to remove or add data sources.
#### 4. Evidence String Processing
- Note: this step is run on a 32 vCPU, 208 GB memory Google Cloud machine
- This step will create the index `${DATA_RELEASE_VERSION}_evidence_data_generic`
#### 5. Association Score Calculation
- Note: this step is run on 32 vCPUs, 208 GB memory Google Cloud machine
- This step will create a new index `${DATA_RELEASE_VERSION}_association-data`
#### 6. Association QC
- This step will get all target-disease pairs from all evidence, and check if they are found in the computed association. 
A list of associations expected but NOT found will be logged.
#### 7. Search Data Processing
- This step will create the index `${DATA_RELEASE_VERSION}_search-data` which is used for the search function in the platform.
#### 8. Relationship Data Computation
- Note: this step is run on 32 vCPUs, 208 GB memory Google Cloud machine.
- This step will compute the target-to-target and disease-to-disease relationships that are stored in the `${DATA_RELEASE_VERSION}_relation-data` index.
#### 9. Generation of Data Metrics (WIP)
- This step produces a flat file with metrics of the current data release which is used to update this 
[page.](https://github.com/opentargets/data_release/wiki/OT011a-Data-Metrics-&-Plots)

--- 
### Running the pipeline locally
Please note that if steps 4, 5, 7 and 8 are run on the full data sets, they are best run on a 32 vCPU, 208 GB Google Cloud machine.

#### 0. Setting up
- Ensure the data_pipeline/db.ini file points to the correct Elasticsearch server: 
`ELASTICSEARCH_NODES = ["http://xxx.xxx.xxx:xxxx"]`
- Ensure the `data_pipeline` branch is corrrect.
- Update the cofiguration files as required (`mrtarget/Settings.py`, `mrtarget/resources/ontology_config.ini` & `mrtarget/resources/uris.ini`)

mrtarget/Settings.py:
```sh
...
ENSEMBL_RELEASE_VERSION = 93
...
```

#### 1. Loading the Base Data
```bash
#Setting Data release version
DATA_RELEASE_VERSION=18.10
#Reactome: For input files see `mrtarget/resources/uris.ini`
python -m mrtarget.CommandLine --rea --log-level DEBUG ${DATA_RELEASE_VERSION}
# Ensembl: Extract via querying the public MySQL database
python -m mrtarget.CommandLine --ens --log-level DEBUG ${DATA_RELEASE_VERSION}
# Uniprot: Extract via querying the REST API (currently changing to downloaded file), return and store in XML format
python -m mrtarget.CommandLine --unic --log-level DEBUG ${DATA_RELEASE_VERSION}
# Baseline Expression: For input files see `mrtarget/resources/uris.ini`
#  RNA: MetaAnalysis provided by the Expression Atlas Team, Protein: Human Protein Atlas
python -m mrtarget.CommandLine --hpa --log-level DEBUG ${DATA_RELEASE_VERSION}
# Mamalian Phenotype Ontology: For input files see `mrtarget/resources/ontology_config.ini`
python -m mrtarget.CommandLine --mp --log-level DEBUG ${DATA_RELEASE_VERSION}
EFO: For input files see `mrtarget/resources/ontology_config.ini`
python -m mrtarget.CommandLine --efo --log-level DEBUG ${DATA_RELEASE_VERSION}
# ECO: For input files see `mrtarget/resources/ontology_config.ini`
python -m mrtarget.CommandLine --eco --log-level DEBUG ${DATA_RELEASE_VERSION}
```
#### 2. Gene merging
```bash
python -m mrtarget.CommandLine --gen --log-level DEBUG ${DATA_RELEASE_VERSION}
```
#### 3. Evidence Data Validation
```bash
# Specify JSON Schema Version
SCHEMA_VERSION=1.2.8
# Validate ALL data sources specified in mrtarget/resources/evidences_sources.txt
python -m mrtarget.CommandLine --val --log-level DEBUG ${DATA_RELEASE_VERSION} \
--schema-version ${SCHEMA_VERSION}
# Validate ONE specific data source
python -m mrtarget.CommandLine --val --log-level DEBUG ${DATA_RELEASE_VERSION} \
--schema-version ${SCHEMA_VERSION} --input-file [PATH_TO_JSON_EVIDENCE_FILE.json.gz]
```
#### 4. Evidence String Processing
> Note: this step is run on a 32 vCPU, 208 GB memory Google Cloud machine
```bash
./launch_mrtarget.sh [container-run-name] [container-branch] --log-level DEBUG ${DATA_RELEASE_VERSION} --evs   
```
#### 5. Association Score Calculation
> Note: this step is run on a 32 vCPU, 208 GB memory Google Cloud machine
```bash
./launch_mrtarget.sh [container-run-name] [container-branch] --log-level DEBUG ${DATA_RELEASE_VERSION} --as
```
#### 6. Association QC
```bash
python -m mrtarget.CommandLine --qc --log-level DEBUG ${DATA_RELEASE_VERSION}
```
#### 7. Search Data Processing
> Note: this step is run on a 32 vCPU, 208 GB memory Google Cloud machine
```bash
./launch_mrtarget.sh [container-run-name] [container-branch] --log-level DEBUG ${DATA_RELEASE_VERSION} --sea
```
#### 8. Relationship Data Computation
> Note: this step is run on a 32 vCPU, 208 GB memory Google Cloud machine
```bash
./launch_mrtarget.sh [container-run-name] [container-branch] --log-level DEBUG ${DATA_RELEASE_VERSION} --ddr   
```
#### 9. Generate Data Metrics
```bash
python -m mrtarget.CommandLine --metric --log-level DEBUG ${DATA_RELEASE_VERSION}
```

---
### Running the pipeline using Docker
Only steps 1, 2, 3, 6 and 9 can be run locally on the complete data. All steps can be run with `--log-level DEBUG`
```sh
# Setting Data release version
DATA_RELEASE_VERSION=18.08
```
#### 1. Loading the Base Data
```bash
#Reactome: For input files see `mrtarget/resources/uris.ini`
./launch_mrtarget.sh [container_run_name] [container_branch] ${DATA_RELEASE_VERSION} --rea
# Ensembl: Extract via querying the public MySQL database
./launch_mrtarget.sh [container_run_name] [container_branch] ${DATA_RELEASE_VERSION} --ens
# Uniprot: Extract via querying the REST API (currently changing to downloaded file), return and store in XML format
./launch_mrtarget.sh [container_run_name] [container_branch] ${DATA_RELEASE_VERSION} --unic
# Baseline Expression: For input files see `mrtarget/resources/uris.ini`
#  RNA: MetaAnalysis provided by the Expression Atlas Team, Protein: Human Protein Atlas
./launch_mrtarget.sh [container_run_name] [container_branch] ${DATA_RELEASE_VERSION} --hpa
# Mamalian Phenotype Ontology: For input files see `mrtarget/resources/ontology_config.ini`
./launch_mrtarget.sh [container_run_name] [container_branch] ${DATA_RELEASE_VERSION} --mp
# EFO: For input files see `mrtarget/resources/ontology_config.ini`
./launch_mrtarget.sh [container_run_name] [container_branch] ${DATA_RELEASE_VERSION} --efo
# ECO: For input files see `mrtarget/resources/ontology_config.ini`
./launch_mrtarget.sh [container_run_name] [container_branch] ${DATA_RELEASE_VERSION} --eco
```
#### 2. Gene merging
```bash
./launch_mrtarget.sh [container_run_name] [container_branch] ${DATA_RELEASE_VERSION} --gen
```
#### 3. Evidence Data Validation
```bash
# Specify JSON Schema Version
SCHEMA_VERSION=1.2.8
# Validate ALL data sources specified in mrtarget/resources/evidences_sources.txt
./launch_mrtarget.sh [container_run_name] [container_branch] ${DATA_RELEASE_VERSION} --val --schema-version ${SCHEMA_VERSION}
# Validate ONE specific data source
./launch_mrtarget.sh [container_run_name] [container_branch] ${DATA_RELEASE_VERSION} --val 
--schema-version ${SCHEMA_VERSION} --input-file [PATH_TO_JSON_EVIDENCE_FILE.json.gz]
# Validate one or more data source(s)
./launch_mrtarget.sh [container_run_name] [container_branch] ${DATA_RELEASE_VERSION} --val 
--schema-version ${SCHEMA_VERSION} --input-file [PATH_TO_JSON_EVIDENCE_FILE.json.gz] 
--input-file [PATH_TO_JSON_EVIDENCE_FILE_2.json.gz]
```
#### 4. Evidence String Processing
> Note: this step is run on a 32 vCPU, 208 GB memory Google Cloud machine
```bash
./launch_mrtarget.sh [container-run-name] [container-branch] --log-level DEBUG ${DATA_RELEASE_VERSION} --evs   
```
#### 5. Association Score Calculation
> Note: this step is run on a 32 vCPU, 208 GB memory Google Cloud machine
```bash
./launch_mrtarget.sh [container-run-name] [container-branch] --log-level DEBUG ${DATA_RELEASE_VERSION} --as
```
#### 6. Association QC
```bash
./launch_mrtarget.sh [container_run_name] [container_branch] --log-level DEBUG ${DATA_RELEASE_VERSION} --qc
```
#### 7. Search Data Processing
> Note: this step is run on a 32 vCPU, 208 GB memory Google Cloud machine
```bash
./launch_mrtarget.sh [container-run-name] [container-branch] --log-level DEBUG ${DATA_RELEASE_VERSION} --sea
```
#### 8. Relationship Data Computation
> Note: this step is run on a 32 vCPU, 208 GB memory Google Cloud machine
```bash
./launch_mrtarget.sh [container-run-name] [container-branch] --log-level DEBUG ${DATA_RELEASE_VERSION} --ddr   
```
#### 9. Generate Data Metrics
```bash
./launch_mrtarget.sh [container-run-name] [container-branch] --log-level DEBUG ${DATA_RELEASE_VERSION} --metric
```

---

## Installation instructions
TO DO: Move these to the relevant sections.

### Useful prep

#### Elasticsearch

You should have an elasticsearch instance running to use the pipeline code. Note that v6 is not 
currently supported, and therefore 5.6 is the latest version (as of writing)
You can run an instance locally using docker containers. 

After deploying elasticsearch, you should check that you can query its API.
Typing `curl localhost:9200` should show something like:
```json
{
  "name": "Wr5vnJs",
  "cluster_name": "elasticsearch",
  "cluster_uuid": "6BlykLd8Sj2mxswcump2wA",
  "version": {
    "number": "5.6.11",
    "build_hash": "bc3eef4",
    "build_date": "2018-08-16T15:25:17.293Z",
    "build_snapshot": false,
    "lucene_version": "6.6.1"
  },
  "tagline": "You Know, for Search"
}
```

#### Kibana

Kibana is useful to browse the output/input of the various steps.

You can install Kibana in a variety of ways, including via [docker](https://www.elastic.co/guide/en/kibana/5.6/docker.html)

**Important:** Kibana version [must be compatible](https://www.elastic.co/support/matrix#show_compatibility) with Elasticsearch.

Once Kibana is installed and deployed, check that it is working by browsing to `http://localhost:5601`


### Package users

Just install via pip `pip install mrtarget` and then you will have a pretty
ready to use application. Just call `mrtarget -h` and it will work as usual
previous data_pipeline commands.

```

### db.ini
The backend (Elasticsearch configuration) can be exported as the ENV var
`ELASTICSEARCH_NODES` as a comma separated list of URLs:
```sh
export ELASTICSEARCH_NODES=http://host1:9200,https://securehost:9243
```

or using a configuration file named `db.ini` where a list
of URL (one for each node if desired) is placed in JSON compatible format:

```sh
[dev]
ELASTICSEARCH_NODES = [
     "https://user:pwd@es.found.io:9200",
     "http://127.0.0.1:9200/"
    ]
```

Normally mrT-arget uses a local instance of redis, thanks to redislite.
To make it connect to any given redis you can specify the parameter `--redis-remote`
and point to the right host and port:
```sh
mrtarget --redis-remote --redis-host '127.0.0.1'--redis-port '8888'
```

This can also be done with environment variables:
```sh
CTTV_REDIS_REMOTE=true
CTTV_REDIS_SERVER=127.0.0.1:8888
```

## Running the Pipeline using Container/Docker

If you have [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/) then you can start Elasticsearch and Kibana in the background with:

```sh
docker-compose up -d elasticsearch kibana
```

By default, these will be accessible on http://localhost:9200 and http://localhost:5601 for Elasticsearch and Kibana respectively.



You can run the pipeline with a command like:

```sh
docker-compose run --rm mrtarget --dry-run my-elasticsearch-prefix
```

or:

```sh
docker-compose run --rm mrtarget --help
```

---
## Using the Makefile

There is a Makefile which can be used to run all or parts of the pipeline, including checking for the existence of required indices.

### Usage

Customise the variables at the top of `Makefile` to suit your needs, then run

`make <target>`

Note that the variables can also be overridden on the command-line.

There are several targets, one for each stage of the pipeline, as well as composite targets, such as 

 * `all`
 * `base`
 * `validate_all`
 
 (see the actual Makefile for the full list, or the output of `make -r -R -p`)

Each target checks that the required Elasticsearch indices exist (via `scripts/check_index.py`) before execution.

There are several targets which speed up common tasks, such as 
 * `list_indices`
 * `clean` (see also `clean_json`, `clean_logs`, `clean_indice`)
 * `shell`
 * `dry_run`

### Notes

*Shell completion*: most shells will complete the list of targets when `<TAB>` is pressed. This is a useful way of seeing which target(s) are available.

*Parallel execution*: `make -j` will run all the dependencies of a target in parallel. Useful for the `load_data` and `validate_all` stages. Using a value will limit to only that number of jobs e.g. `-j 4` will limit to 4. Using `-l x` will only create new jobs if the total load on the machine is below that threashold - usefuul as several of the stages themselves run over multiple processess. These can be combined - for example `make -j 8 -l 4` will spawn up to 8 jobs at the same time as long as the load is less than 4 when creating them. Note that when commands that use Redis are run in parallel, they will each try to start an embedded Redis on the same port and all fail; to solve this, use an shared external Redis instance.

*Variables*: the default values for the variables set at the top of the `Makefile` can be overridden on the command-line, e.g. `make "ES_PREFIX=test" ens` Note that if any of these are already specified by environment variables, those values will take precedence. There is a `MRTARGET_ARGS` variable which can be used to specify arbitrary additional parameters at run time. 

*Partial execution*: the targets inside the makefile use absolute paths. While this is useful for running the makefile from a directory outside of the root of the project,
when only a partial execution is desired (e.g. for testing) then the full path will be required.

*Parital data*: By default, the makefile will only download and process up to 1000 evidence strings per data source. This is useful to keep the data processing to a manageable size and duration, but when used in production it is necessary to set it to a much higher value e.g. 
`make "NUMBER_TO_KEEP=10000000" all`

*Recovery*: Make is designed around files, and regneerating them when out of date. To work with the OpenTargets pipeline, the files it is based on are the log files produced by the various stages. This means that if you need to rerun a stage (e.g. to regenerate an Elasticsearch index) you will need to delete the appropriate log file and re-run make.

## Using the Makefile within Docker

It is possible to use both docker and the makefile together. You will need to override the default entrypoint of the docker image. For example:

```sh
docker-compose run --rm --entrypoint make mrtarget -j -l 8 -r -R -k all
```

---

## Environment variables and how to use them

TO DO: Check if these are still correct and relevant.

Here the list to change, enable or disable functionality:

* `CTTV_EV_LIMIT` string `true` to enable otherwise `false` or delete (WIP)
* `CTTV_MINIMAL` string `true` to enable otherwise `false` or delete
* `CTTV_DATA_VERSION` indexes prefix a name to enable otherwise `17.04`
* `CTTV_EL_LOADER` the name of the section of the ES config file db.ini default `dev`
* `ELASTICSEARCH_NODES` default is `['http://127.0.0.1:9200']`
* `CTTV_WORKERS_NUMBER` is an number and by default is `multiprocessing.cpu_count()`
* `CTTV_DUMP_FOLDER` is a string of a writable path and by default is `tempfile.gettempdir()`
* `DUMP_REMOTE_API_URL` a string and by default is `http://beta.opentargets.io`
* `DUMP_REMOTE_API_PORT` a number and by default is `80`
* `DUMP_REMOTE_API_SECRET` a string and by default is `None`
* `DUMP_REMOTE_API_APPNAME` a string and by default is `None`
* `CTTV_DRY_RUN_OUTPUT` string `true` to enable otherwise is `false` by default
* `CTTV_DRY_RUN_OUTPUT_DELETE` string `true` to enable otherwise is `false` by fefault
* `CTTV_DRY_RUN_OUTPUT_COUNT` number `1000` by default
* `CTTV_ES_CUSTOM_IDXS` `true` to enable it and by default is `false`

## Contributing

Here is the recipe to start coding on it:

```bash
$ git clone ${this_repo}
$ cd ${this_repo}
```
Add a file `mrtarget/resources/db.ini` with contents similar to:
```bash
[dev]
ELASTICSEARCH_NODES = [
     "http://127.0.0.1:9200/"
    ]
```
Install python dependencies and test run:
```bash
$ # create a new virtualenv 2.7.x (you know how to do it, right?)
$ pip install -r requirements.txt
$ mrtarget -h
```

Now you are ready to contribute to this project. Note that the **resource files** are located in
the folder `mrtarget/resources/`.

Tests are run using pytest. For example:
```sh
pytest tests/test_score.py
```

To skip running tests in the CI when pushing, append `[skip ci]` to the commit message.

The elasticsearch guide can be very useful: https://www.elastic.co/guide/en/elasticsearch/guide/2.x/getting-started.html
The important bits to run/understand the data pipeline are the `getting started`, `search in depth`, `aggregations`, and `modeling your data`.

### Internal features for developers

#### Evidence Validation (--val step) limit (WIP)

It will give up if too many evidence strings failed the JSON schema validation. 1000 docs is a
reasonable limit per launched process.

#### Minimal dataset

In case you want to run steps with a minimal dataset you should specify `export CTTV_MINIMAL=true`. If you want
to regenerate the data source files that were used, then run:

```bash
$ cd scripts/
$ bash generate_minimal_dataset.sh
```

It should upload to gcloud storage and enable public links to files but you need to be logged in to the gcloud account.

#### Redislite and/or remote redis

--redis-remote 'enable remote'
--redis-host 'by example 127.0.0.1'
--redis-port 'by example 8888'

CTTV_REDIS_REMOTE=true
CTTV_REDIS_SERVER=127.0.0.1:8888

If you do not specify remote then it will try to bind to that host and port and the
overwrite rule is env var then arguments overwrite

# Copyright

Copyright 2014-2018 Biogen, Celgene Corporation, EMBL - European Bioinformatics Institute, GlaxoSmithKline, Sanofi, Takeda Pharmaceutical Company and Wellcome Sanger Institute

This software was developed as part of the Open Targets project. For more information please see: http://www.opentargets.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
See the License for the specific language governing permissions and
limitations under the License.

