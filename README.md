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
Sources of evidence strings that are processed by the `--evs` steps by default are specified in a [config file](https://github.com/opentargets/data_pipeline/blob/master/mrtarget/resources/evidences_sources.txt)

We save them in a gs:// bucket, so to make up the file you can just run:
```sh
gsutil ls gs://ot-releases/17.12 | sed 's/gs/http/' | sed 's/\/\//\/\/storage.googleapis.com\//' | sed '1d'
```

## Installation instructions

### Useful prep

#### Elasticsearch

You should have an elasticsearch instance running to use the pipeline code. 
You can run an instance locally using docker containers. You can use our ES v2.4 docker containers ([repo](https://github.com/opentargets/docker-elasticsearch-singlenode) | [![Docker Repository on Quay](https://quay.io/repository/opentargets/docker-elasticsearch-singlenode/status "Docker Repository on Quay")](https://quay.io/repository/opentargets/docker-elasticsearch-singlenode) )
```sh
docker run -p 9200:9200 -e NUMBER_OF_REPLICAS=0 quay.io/opentargets/docker-elasticsearch-singlenode:v2.4.1
```
Do take a look at the repo for other options you can pass to the container. 
If this is anything more than a test, you will want to change the ES_HEAP, etc. 

After deploying elasticsearch, you should check that you can query its API. 
Typing `curl localhost:9200` should show something like: 
```json
{
  "name": "Kenneth Crichton",
  "cluster_name": "open-targets-dev",
  "version": {
    "number": "2.4",
    "build_hash": "218bdf10790eef486ff2c41a3df5cfa32dadcfde",
    "build_timestamp": "2016-05-17T15:40:04Z",
    "build_snapshot": false,
    "lucene_version": "5.5.0"
  },
  "tagline": "You Know, for Search"
}
```

#### Kibana

Kibana, marvel and sense are useful tools to monitor the performance of mrTarget, and to browse the output/input of the various steps.

You can install kibana in a variety of ways. **Important:** your version of kibana [must be compatible](https://www.elastic.co/support/matrix#show_compatibility) with the version of ES we are using. 

On mac: 
```sh
brew install kibana

#Install the sense plugin
kibana plugin --install elastic/sense
#Install marvel 
kibana plugin --install elasticsearch/marvel

#start the service
brew services start kibana
```

Using docker:

For ES>5, you can use the official [docker images](https://www.elastic.co/guide/en/kibana/current/_pulling_the_image.html)
```sh
docker run docker.elastic.co/kibana/kibana:5.5.2 -e "ELASTICSEARCH_URL=http://localhost:9200"
```

For ES2.4:
```sh
docker run kibana:4.6 -e "ELASTICSEARCH_URL=http://localhost:9200"
```

Once kibana is installed and deployed, check that it is working by browsing to `http://localhost:5601`


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

#### Dry-run to file

TBD

#### Custom read indexes

TBD

#### Redislite and/or remote redis

--redis-remote 'enable remote'
--redis-host 'by example 127.0.0.1'
--redis-port 'by example 8888'

CTTV_REDIS_REMOTE=true
CTTV_REDIS_SERVER=127.0.0.1:8888

if you dont specify remote then it will try to bind to that host and port and the
overwrite rule is env var then arguments overwrite

