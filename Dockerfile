FROM python:2.7

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# this should also copy the venv created by circleCI
ADD . /usr/src/app/

CMD venv/bin/python mrtarget
