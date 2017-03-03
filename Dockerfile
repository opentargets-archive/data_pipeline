FROM python:2.7-onbuild

RUN python -m spacy.en.download 
RUN python -m nltk.downloader all-corpora

CMD [ "python", "./run.py",  "--help" ]