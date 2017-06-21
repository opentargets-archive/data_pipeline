FROM python:2.7

# install docker client - necessary for docker in docker
# RUN apt-get update; \
#     apt-get install -y --no-install-recommends \
#      apt-transport-https \
#      gnupg2 \
#      software-properties-common

# git
# ssh
# tar
# gzip

ENV DOCKER_VERSION 17.03.0-ce

RUN set -x \
    && curl -L -o /tmp/docker-$DOCKER_VERSION.tgz https://get.docker.com/builds/Linux/x86_64/docker-$DOCKER_VERSION.tgz \
    && tar -xz -C /tmp -f /tmp/docker-$DOCKER_VERSION.tgz \
    && mv /tmp/docker/* /usr/bin

# RUN curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -
# RUN add-apt-repository \
#    "deb [arch=amd64] https://download.docker.com/linux/debian \
#    $(lsb_release -cs) \
#    stable"
# RUN apt-get update
# RUN apt-get install docker-ce="17.03.0-ce"

# do what onbuild would have done
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
