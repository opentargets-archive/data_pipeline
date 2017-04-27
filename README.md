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
