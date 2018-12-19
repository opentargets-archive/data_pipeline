#!/usr/bin/env bash

# This is a script to execute the opentargets data_pipeline
# on a google cloud platform machine. Such a machine will be created
# but you need to have the Cloud SDK https://cloud.google.com/sdk/
# appropriately setup and configured first.



set -x # echo commands to terminal
set -e # fail on error
set -u # unset variables are errors
set -o pipefail # a pipe fails if any command in it fails
shopt -s failglob # empty globs are errors

NOW=`date +'%y%m%d-%k%M%S'`
#underscores are not allowed
NAME=data-pipeline-18-12-3-$NOW

#create an instance
#  n1-standard-8 is 8 vCPUs and 30GB memory
#  n1-standard-16 is 16 vCPUs and 60GB memory
#  n1-standard-32 is 32 vCPUs and 120GB memory
#  n1-highcpu-16 is 16 vCPUs and 14.4GB memory
#  n1-highcpu-32 is 32 vCPUs and 28.8GB memory
#  n1-highcpu-64 is 64 vCPUs and 57.6GB memory
#  n1-highmem-8 is 8 vCPUs and 52GB memory
#  n1-highmem-16 is 16 vCPUs and 104GB memory
#  n1-highmem-32 is 32 vCPUs and 208GB memory
#Note: for 18.12 release more than 60GB memory is required

#TODO check if it exists already and abort
gcloud compute instances create $NAME \
  --image-project debian-cloud \
  --image-family debian-9 \
  --machine-type n1-highmem-16 \
  --boot-disk-size 250 \
  --boot-disk-type pd-ssd --boot-disk-device-name $NAME
  
#sleep a bit to allow it to start fully
sleep 30

#debian doesn't have docker pre-installed, so add it
#docker-compose needs to exist too
#and we need to tweak a kernel seting for elasticsearch
gcloud compute ssh $NAME --command "sudo bash" <<EOF
set -e # fail on error
set -x # echo commands to terminal
set -u # unset variables are errors
set -o pipefail # a pipe fails if any command in it fails
shopt -s failglob # empty globs are errors

sudo apt-get -q -y update
sudo apt-get -q -y install \
     apt-transport-https \
     ca-certificates \
     curl \
     gnupg2 \
     software-properties-common
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian jessie stable"
sudo apt-get -q -y update
sudo apt-get -q -y install docker-ce
sudo systemctl enable docker

#update docker-compose version number below as appropriate
sudo curl -L "https://github.com/docker/compose/releases/download/1.23.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

#elasticsearch needs a large number of concurrent files
sudo echo 'vm.max_map_count=262144' >> /etc/sysctl.conf
sudo sysctl -p
echo "vm.max_map_count is `sudo sysctl vm.max_map_count`"
sudo echo '* hard nofile 10000' >> /etc/security/limits.conf
sudo echo '* hard memlock infinity' >> /etc/security/limits.conf
sudo echo /etc/security/limits.conf

EOF


#add a docker compose file to the remote machine
gcloud compute ssh $NAME --command "cat > docker-compose.yml" <<EOF
version: "3.6"
services:
      
  elasticsearch:
    #image: elasticsearch:5.6.13
    #note that the docker hub version doesn't allow restores for some reason...
    image: docker.elastic.co/elasticsearch/elasticsearch:5.6.13
    ports:
      - 9200:9200
    environment:
      # assign more memory to JVM 
      - "ES_JAVA_OPTS=-Xms12g -Xmx12g"
      #disable xpack as not OSS
      - "xpack.security.enabled=false"
      #disable memory swapping to disk for performance
      - "bootstrap.memory_lock=true"
      #increase the buffer is used to store newly indexed documents
      #defaults to 10% of heap but more is better for heavy indexing
      - "indices.memory.index_buffer_size=4g"
      #allow downloading from google buckets
      - repositories.url.allowed_urls=https://storage.googleapis.com/*,https://*.amazonaws.com/*
    volumes:
      #use a volume for persistence / performance
      - esdata:/usr/share/elasticsearch/data
    ulimits:
      #disable memory swapping to disk for performance
      memlock:
        soft: -1
        hard: -1
    healthcheck:
      test: ["CMD", "curl", "http://localhost:9200"]
      interval: 30s
      timeout: 500s
      retries:  30
        
  kibana:
    image: kibana:5.6.13
    ports:
      - 5601:5601
    environment:
      #disable xpack as not OSS
      - xpack.security.enabled=false
    depends_on:
      - elasticsearch

  redis:
    image: redis
    ports:
      - 6379:6379
    
  mrtarget:
    image: quay.io/opentargets/mrtarget:latest
    depends_on:
      - elasticsearch
      - redis
    environment:
      - ELASTICSEARCH_NODES=http://elasticsearch:9200
      - CTTV_REDIS_REMOTE=true
      - CTTV_REDIS_SERVER=redis:6379
      - SCHEMA_VERSION=1.3.0
      - NUMBER_TO_KEEP=100000000
      - ES_PREFIX=latest
    volumes:
      - ./log:/usr/src/app/log
      - ./json:/usr/src/app/json
      - ./qc:/usr/src/app/qc

  ot_api:
    image: quay.io/opentargets/rest_api
    ports:
      - 8080:80
    depends_on:
      - elasticsearch
    environment:
      - ELASTICSEARCH_URL=http://elasticsearch:9200
      - OPENTARGETS_DATA_VERSION=latest
    healthcheck:
      test: ["CMD", "curl", "http://localhost:80/v3/platform/public/utils/stats"]
      interval: 30s
      timeout: 500s
      retries:  30

  ot_webapp:
    image: quay.io/opentargets/webapp
    ports:
      - 8081:80
      - 7443:443
    depends_on:
      - ot_api
    environment:
      - REST_API_SCHEME=http
      - REST_API_SERVER=ot_api
      - REST_API_PORT=80
      - APIHOST=localhost
    healthcheck:
      test: ["CMD", "curl", "https://localhost:80/api/v3/platform/public/utils/stats"]
      interval: 30s
      timeout: 500s
      retries:  30
      
volumes:
  esdata:
    driver: local

EOF

#start services (elastic, redis), then wait a bit
gcloud compute ssh $NAME --command 'sudo docker-compose up -d elasticsearch redis'
sleep 30

#Connect to the machine and use docker compose to run the pipeline

#Dry run first
gcloud compute ssh $NAME --command 'sudo docker-compose run --rm --entrypoint make mrtarget dry_run'
    
#full build!
#metrics is broken, so dont do it
gcloud compute ssh $NAME --command "cat > run.sh" <<EOF
docker-compose run -d --rm --entrypoint make mrtarget -r -R -j -l 16 relationship_data search_data association_qc
EOF
gcloud compute ssh $NAME --command "chmod +x run.sh"
#this will take about 18h to build
gcloud compute ssh $NAME --command 'sudo ./run.sh'


#delete the instance at the end
echo "ONCE FINISHED, DELETE WITH:"
echo gcloud compute instances delete -q $NAME
