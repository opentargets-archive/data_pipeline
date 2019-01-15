[![Build Status](https://travis-ci.com/opentargets/data_pipeline.svg?branch=master)](https://travis-ci.com/opentargets/data_pipeline)

[![Docker Repository on Quay.io](https://quay.io/repository/opentargets/mrtarget/status "Docker Repository on Quay.io")](https://quay.io/repository/opentargets/mrtarget)

## MrT-arget

All code related with the computation of the process needed to
complete the pipeline are here refactored into a python package.

We are also building a container with all the python (and nonpython)
dependencies that allows you to run each step of the pipeline.


### How do I decide which data sources to include?
Sources of evidence strings that are processed by the `--evs` steps by 
default are specified in a [config file](https://github.com/opentargets/data_pipeline/blob/master/mrtarget/resources/evidences_sources.txt)

We save them in a gs:// bucket, so to make up the file you can just run:
```sh
gsutil ls gs://ot-releases/17.12 | sed 's/gs/http/' | sed 's/\/\//\/\/storage.googleapis.com\//' | sed '1d'
```

## Installation instructions

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
    "number": "5.6.13",
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



### Container/Docker users


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

#### development with docker-compose

By default, the docker compose file will *not use a locally built image* because it will download the latest image from [quay.io/opentargets/mrtarget](https://www.quay.io/repository/opentargets/mrtarget). So any changes made will not be applied by default.

Docker-compose has the ability to layer multiple docker-compose.yml files together; by default, `docker-compose.override.yml` will be added to `docker-compose.yml`. This can be used to use an override to build the image locally i.e.:

```sh
docker-compose run --rm -f docker-compose.yml -f docker-compose.dev.yml mrtarget --dry-run my-elasticsearch-prefix
```

This is done because overrides cannot remove previous values, so once a build directive has been specified it will always be used. Therefore, the build instruction must be outside of the default docker-compose.yml to support cases where the pipeline should be run but not built.

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


### Using the Makefile within Docker

It is possible to use both docker and the makefile together. You will need to override the default entrypoint of the docker image. For example:

```sh
docker-compose run --rm --entrypoint make mrtarget -j -l 8 -r -R -k all
```

As discussed above, by default, the docker compose file will *not use a locally built image*. See above for how to work with this.

## Putting it all together on Google Cloud Platform

There is a script `scripts/run_on_gcp.sh` that puts together the information above to create a virtual machine on Google Cloud Platform (GCP), install Docker and Docker Compose, and execute the pipeline via the Makefile within a Docker container. The only prerequisite is [Google Cloud SDK](https://cloud.google.com/sdk/docs/quickstarts) (gcloud) and then run `scripts/run_on_gcp.sh`.

This will run a tagged version, so if you want use something else or to make your own changes, then you'll need to do more in-depth investigation. Note, it is not fast and will take on the order of 12 to 36 hours.

---

## Environment variables and how to use them

Here the list to change, enable or disable functionality:

* `CTTV_EV_LIMIT` string `true` to enable otherwise `false` or delete (WIP)
* `CTTV_MINIMAL` string `true` to enable otherwise `false` or delete
* `CTTV_DATA_VERSION` indexes prefix a name to enable otherwise `17.04`
* `CTTV_EL_LOADER` the name of the section of the ES config file db.ini default `dev`
* `ELASTICSEARCH_NODES` default is `['http://127.0.0.1:9200']`
* `WORKERS_NUMBER` is an number and by default is `multiprocessing.cpu_count()`
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

Here the recipe to start coding on it:

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

Now you are ready to contribute to this project. As note, the **resource files** are located in
the folder `mrtarget/resources/`.

Tests are run using pytest. For eg:
```sh
pytest tests/test_score.py
```

To skip running tests in the CI when pushing append `[skip ci]` to the commit message.

The elasticsearch guide can be very useful: https://www.elastic.co/guide/en/elasticsearch/guide/2.x/getting-started.html
The important bits to run/understand the data pipeline are the `getting started`, `search in depth`, `aggregations`, and `modeling your data`. You can probably ignore the others.

### Profiling with PyFlame

There is some additional configuration to build a docker container that can run the pipeline while profiling it with [PyFlame ](https://github.com/uber/pyflame) to produce
output that [FlameGraph](https://github.com/brendangregg/FlameGraph) can turn into pretty interactive svg images.

To do this, use the `Dockerfile.pyfile` to build the container, and run it ensuring that the kernel of the host has `kernel.yama.ptrace_scope=0` and `--cap-add=SYS_PTRACE` on the docker container. See the updated entrypoint `scripts/entrypoint.pyflame.sh` for more details.

It will output to `logs/profile.*.svg` which can be opened with a browser e.g. Chrome. Note, while profiling performance will be **much much slower**.

# Copyright

Copyright 2014-2018 Biogen, Celgene Corporation, EMBL - European Bioinformatics Institute, GlaxoSmithKline, Takeda Pharmaceutical Company and Wellcome Sanger Institute

This software was developed as part of the Open Targets project. For more information please see: http://www.opentargets.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

