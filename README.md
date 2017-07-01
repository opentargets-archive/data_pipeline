[![CircleCI](https://circleci.com/gh/opentargets/data_pipeline.svg?style=svg&circle-token=e368180959ed512016dbfe75ec65814e896e0aea)](https://circleci.com/gh/opentargets/data_pipeline)

Docker containers are saved on [quay.io](https://quay.io/repository/opentargets/mrtarget?tab=tags):
[![Docker Repository on Quay](https://quay.io/repository/opentargets/mrtarget/status?token=7cd783a9-247c-4625-ae97-e0933192b2f4 "Docker Repository on Quay")](https://quay.io/repository/opentargets/mrtarget)

and [eu.gcr.io](https://console.cloud.google.com/gcr/images/open-targets/EU/mrtarget?project=open-targets)  (which has no badge sadly).

## MrT-arget

Practically, all code related with the computation of the process needed to
complete the pipeline are here refactored into a proper package.

## Installation instructions

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

### Container users

```sh
docker run eu.gcr.io/open-targets/mrtarget:master mrtarget --dry-run
## or using quay.io
docker run quay.io/opentargets/mrtarget:master mrtarget --dry-run
```
You probably want to mount log files, etc like we do in our [backend machine](https://github.com/opentargets/infrastructure/blob/master/gcp/cloud-config/be-worker-cos.yaml):

```sh
docker run -d --name mrtarget_master \
        -e TERM=xterm-256color \
        -v `pwd`/data:/tmp/data \
        -v `pwd`/output_master.log:/usr/src/app/output.log \
        -v `pwd`/third_party_master.log:/usr/src/app/thirdparty.log \
        -v `pwd`/db.ini:/usr/src/app/db.ini \
        -v `pwd`/es_custom_idxs.ini:/usr/src/app/es_custom_idxs.ini \
        -e CTTV_EL_LOADER=dev \
        -e CTTV_DATA_VERSION=<dataversion> \
        -e CTTV_DUMP_FOLDER=/tmp/data \
        -e CTTV_ES_CUSTOM_IDXS=true \
        eu.gcr.io/open-targets/mrtarget:master \
        mrtarget --dry-run
```


### Package developers

Here the recipe to start coding on it:

```bash
$ git clone ${this_repo}
$ cd ${this_repo}
$ # create a new virtualenv 2.7.x (you know how to do it, right?)
$ pip install -r requirements.txt
$ python setup.py install
$ python -m spacy.en.download
$ python -m nltk.downloader all-corpora
$ mrtarget -h
```

Now you are ready to contribute to this project. As note, the **resource files** are located in
the folder `mrtarget/resources/`.

Tests are run using pytest. For eg:
```sh
pytest tests/test_score.py
```

To skip running tests in the CI when pushing append `[skip ci]` to the commit message.

### Environment variables and howto use them

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

## Internal features for developers

### Evidence Validation (--val step) limit (WIP)

It will give up if too many evidences were failed under schema validation. 1000 docs will be a
rasonable upper bound limit per launched process.

### Minimal dataset

In case you want to run steps with a minimal dataset you should `export CTTV_MINIMAL=true`. If you want
to regenerate used data source files:

```bash
$ cd scripts/
$ bash generate_minimal_dataset.sh
```

It should upload to gcloud storage and enable public link to files but you need to be logged
in the gcloud account.

### Dry-run to file

TBD

### Custom read indexes

TBD

### Redislite and/or remote redis

--redis-remote 'enable remote'
--redis-host 'by example 127.0.0.1'
--redis-port 'by example 8888'

CTTV_REDIS_REMOTE=true
CTTV_REDIS_SERVER=127.0.0.1:8888

if you dont specify remote then it will try to bind to that host and port and the
overwrite rule is env var then arguments overwrite

