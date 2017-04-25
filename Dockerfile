FROM python:2.7

RUN apt-get update && apt-get install -y --no-install-recommends double-conversion && rm -rf /var/lib/apt/lists/*
RUN pip install mrtarget
RUN python -m spacy.en.download
RUN python -m nltk.downloader all-corpora

CMD [ "mrtarget", "--help" ]
