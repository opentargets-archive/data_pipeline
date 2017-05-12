#!/bin/bash

# convert from ensemble id to uniprotid then
# ensemble and filter the sql query
# and filter evidencestring from those ensemble ids

rm -Rf output
rm -Rf data
mkdir output

#gene list
genes=$(cat ../mrtarget/resources/minimal_ensembl.txt)

url_file="urls.txt"

cat ../mrtarget/resources/uris.ini | \
    grep -E "(proteinatlas\.org|reactome\.org|ebi\.ac\.uk\/chembl|\.zip)" | \
    sed -e 's/.*\=//g' > $url_file

wget -P data/ -i urls.txt
wait $!

rm -f $url_file

sources=$(ls -1 data/)
for src in $sources
do
    IFS=. read filename filext filerest <<< $src

    new_source="output/minimal_${filename}.${filext}"
    CMD="cat"
    if [[ $src =~ .*\.(zip|gz) ]]
    then
        CMD="zcat"
    fi

    $CMD data/$src | head -n 1 > $new_source

    for gene in $genes
    do
        echo data/$src $gene
        $CMD data/$src | grep $gene >> $new_source
    done
done

echo "uploading new dataset to gcloud"
gsutil rsync -d output gs://ot-input-dev
gsutil acl ch -u AllUsers:R gs://ot-input-dev/*

rm -Rf output
rm -Rf data

