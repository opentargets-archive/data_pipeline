#!/bin/bash

export CTTV_MINIMAL=true
export CTTV_DATA_VERSION=minimaltest

cd ..

source /usr/local/bin/virtualenvwrapper.sh
workon data_pipeline
# # independent steps
# python -m mrtarget.CommandLine --hpa &
# python -m mrtarget.CommandLine --rea &
# python -m mrtarget.CommandLine --uni &
# python -m mrtarget.CommandLine --ens &
# python -m mrtarget.CommandLine --efo &
# python -m mrtarget.CommandLine --eco &

# # wait for previous commands
# wait

# python -m mrtarget.CommandLine --gen

python -m mrtarget.CommandLine --val --remote-file https://storage.googleapis.com/otar001-core/17.04/cttv001_gene2phenotype-26-04-2017.json.gz
killall redis-server
python -m mrtarget.CommandLine --val --remote-file https://storage.googleapis.com/otar001-core/17.04/cttv001_intogen-26-04-2017.json.gz
killall redis-server
python -m mrtarget.CommandLine --val --remote-file https://storage.googleapis.com/otar001-core/17.04/cttv001_phenodigm-26-04-2017.json.gz
killall redis-server
python -m mrtarget.CommandLine --val --remote-file https://storage.googleapis.com/otar006-reactome/cttv006-04-04-2017.json.gz
killall redis-server
python -m mrtarget.CommandLine --val --remote-file https://storage.googleapis.com/otar007-cosmic/cttv007-17-02-2017.json.gz
killall redis-server
python -m mrtarget.CommandLine --val --remote-file https://storage.googleapis.com/otar008-chembl/cttv008-12-04-2017.json.gz
killall redis-server
python -m mrtarget.CommandLine --val --remote-file https://storage.googleapis.com/otar009-gwas/17.04/cttv009-23-03-2017.json.gz
killall redis-server
python -m mrtarget.CommandLine --val --remote-file https://storage.googleapis.com/otar010-atlas/cttv010-25-04-2017.json.gz
killall redis-server
python -m mrtarget.CommandLine --val --remote-file https://storage.googleapis.com/otar011-uniprot/cttv011-24-03-2017.json.gz
killall redis-server
python -m mrtarget.CommandLine --val --remote-file https://storage.googleapis.com/otar012-eva/cttv012-02-02-2017.json.gz
killall redis-server
python -m mrtarget.CommandLine --val --remote-file https://storage.googleapis.com/otar025-epmc/cttv025-05-04-2017.json
killall redis-server

deactivate

exit 0
