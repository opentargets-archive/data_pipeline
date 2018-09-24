CircleCI build status: [![CircleCI](https://circleci.com/gh/opentargets/data_pipeline.svg?style=svg&circle-token=e368180959ed512016dbfe75ec65814e896e0aea)](https://circleci.com/gh/opentargets/data_pipeline)

Docker containers are saved on [quay.io](https://quay.io/repository/opentargets/mrtarget?tab=tags):
[![Docker Repository on Quay](https://quay.io/repository/opentargets/mrtarget/status?token=7cd783a9-247c-4625-ae97-e0933192b2f4 "Docker Repository on Quay")](https://quay.io/repository/opentargets/mrtarget)

and [eu.gcr.io](https://console.cloud.google.com/gcr/images/open-targets/EU/mrtarget?project=open-targets)  (which has no badge sadly).

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



### Container users


If you have [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/) then you can start Elasticsearch and Kibana with:

```sh
docker-compose up elasticsearch kibana
```

You can run the pipeline with a command like:

```sh
ES_PREIFX=my-elasticsearch-prefix
docker-compose run --rm mrtarget --dry-run $ES_PREFIX
```


## Environment variables and howto use them

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

### Internal features for developers

#### Evidence Validation (--val step) limit (WIP)

It will give up if too many evidences were failed under schema validation. 1000 docs will be a
rasonable upper bound limit per launched process.

#### Minimal dataset

In case you want to run steps with a minimal dataset you should `export CTTV_MINIMAL=true`. If you want
to regenerate used data source files:

```bash
$ cd scripts/
$ bash generate_minimal_dataset.sh
```

It should upload to gcloud storage and enable public link to files but you need to be logged
in the gcloud account.

#### Redislite and/or remote redis

--redis-remote 'enable remote'
--redis-host 'by example 127.0.0.1'
--redis-port 'by example 8888'

CTTV_REDIS_REMOTE=true
CTTV_REDIS_SERVER=127.0.0.1:8888

if you dont specify remote then it will try to bind to that host and port and the
overwrite rule is env var then arguments overwrite

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

