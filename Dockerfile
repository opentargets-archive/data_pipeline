FROM python:2.7

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# this should also copy the venv created by circleCI
ADD . /usr/src/app/

# use the virtualenv 
# a COMPLETE overkill in docker, but I am doing it to leverage the CircleCI
# caching system
CMD venv/bin/python
