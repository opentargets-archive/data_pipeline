[![wercker status](https://app.wercker.com/status/e2b4ec985b6d6f6945fe9dd54441bca6/m/master "wercker status")](https://app.wercker.com/project/byKey/e2b4ec985b6d6f6945fe9dd54441bca6)

## This package is called MrT-arget

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

