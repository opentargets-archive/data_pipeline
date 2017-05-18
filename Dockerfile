FROM python:2.7-onbuild


RUN apt-get update && apt-get install -y --no-install-recommends double-conversion && rm -rf /var/lib/apt/lists/*
RUN python -m spacy.en.download 

CMD [ "python", "./run.py",  "--help" ]