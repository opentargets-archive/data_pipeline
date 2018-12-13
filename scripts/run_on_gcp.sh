#!/usr/bin/env bash
set -x # echo commands to terminal
set -e # fail on error
set -u # unset variables are errors
set -o pipefail # a pipe fails if any command in it fails
shopt -s failglob # empty globs are errors

NOW=`date +'%y%m%d-%k%M%S'`
#underscores are not allowed
NAME=data-pipeline-$NOW
#cores should be at least 16 to get >100GB RAM
CORES=32

#create an instance
#TODO check if it exists already and abort
gcloud compute instances create $NAME \
  --image-project debian-cloud \
  --image-family debian-9 \
  --machine-type n1-highmem-$CORES \
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

sudo sysctl -w vm.max_map_count=262144
sudo echo '* hard nofile 10000' >> /etc/security/limits.conf
sudo echo '* hard memlock infinity' >> /etc/security/limits.conf
sudo echo /etc/security/limits.conf

EOF


#add a docker compose file to the remote machine
gcloud compute ssh $NAME --command "cat > docker-compose.yml" <<EOF
version: "3.6"
services:
      
  elasticsearch:
    #image: elasticsearch:5.6.11
    #note that the docker hub version doesn't allow restores for some reason...
    image: docker.elastic.co/elasticsearch/elasticsearch:5.6.13
    ports:
      - 9200:9200
    environment:
      # assign more memory to JVM 
      - "ES_JAVA_OPTS=-Xms12g -Xmx12g"
      - "xpack.security.enabled=false"
      - "bootstrap.memory_lock=true"
      #allow downloading from google buckets
      - repositories.url.allowed_urls=https://storage.googleapis.com/*,https://*.amazonaws.com/*
    volumes:
      #use a volume for persistence / performance
      - esdata:/usr/share/elasticsearch/data
    ulimits:
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
      - xpack.security.enabled=false
    depends_on:
      - elasticsearch

  redis:
    image: redis
    ports:
      - 6379:6379
    
  mrtarget:
    image: quay.io/opentargets/mrtarget:18.10.2
    depends_on:
      - elasticsearch
      - redis
    environment:
      - ELASTICSEARCH_NODES=http://elasticsearch:9200
      - CTTV_REDIS_REMOTE=true
      - CTTV_REDIS_SERVER=redis:6379
      - SCHEMA_VERSION=1.3.0
      - NUMBER_TO_KEEP=100000000
    volumes:
      - ./log:/usr/src/app/log
      - ./json:/usr/src/app/json
      - ./qc:/usr/src/app/qc

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
gcloud compute ssh $NAME --command 'sudo docker-compose run -d --rm --entrypoint make mrtarget -r -R all'


#delete the instance at the end
echo "ONCE FINISHED, DELETE WITH:"
echo gcloud compute instances delete -q $NAME
