[![wercker status](https://app.wercker.com/status/e2b4ec985b6d6f6945fe9dd54441bca6/m/master "wercker status")](https://app.wercker.com/project/byKey/e2b4ec985b6d6f6945fe9dd54441bca6)

## This package is called MrT-arget

Practically, all code related with the computation of the process needed to
complete the pipeline are here refactored into a proper package.

## Installation instructions

### Package users

Just install via pip `pip install mrtarget` and then you will have a pretty
ready to use application. Just call `mrtarget -h` and it will work as usual
previous data_pipeline commands.

*Do not forget to install this once after `mrtarget` installation:

```bash
$ python -m spacy.en.download
$ python -m nltk.downloader all-corpora

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

Now you are ready to contribute to this project.
