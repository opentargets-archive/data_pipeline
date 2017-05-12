#!/bin/bash

# convert from ensemble id to uniprotid then
# ensemble and filter the sql query
# and filter evidencestring from those ensemble ids

#gene list
genes="ENSG00000169194 ENSG00000073756 ENSG00000073605 ENSG00000110324 ENSG00000085978 ENSG00000105397 ENSG00000142192 ENSG00000080815 ENSG00000164885 ENSG00000147889 ENSG00000157764 ENSG00000134460"

sources="normal_tissue.csv.zip cancer.csv.zip subcellular_location.csv.zip rna_tissue_atlas.csv.zip Ensembl2Reactome.txt ReactomePathways.txt ReactomePathwaysRelation.txt target.json mechanism.json protein_class.json target_component.json"

url_file="urls.txt"
cat > $url_file << EOF
# urls to fetch and filter
http://v16.proteinatlas.org/download/normal_tissue.csv.zip
http://v16.proteinatlas.org/download/cancer.csv.zip
http://v16.proteinatlas.org/download/subcellular_location.csv.zip
https://storage.googleapis.com/atlas_baseline_expression/rna_tissue_atlas.csv.zip
http://www.reactome.org/download/current/Ensembl2Reactome.txt
http://www.reactome.org/download/current/ReactomePathways.txt
http://www.reactome.org/download/current/ReactomePathwaysRelation.txt
https://www.ebi.ac.uk/chembl/api/data/target.json
https://www.ebi.ac.uk/chembl/api/data/mechanism.json
https://www.ebi.ac.uk/chembl/api/data/protein_class.json
https://www.ebi.ac.uk/chembl/api/data/target_component.json

EOF

wget -P data/ -i urls.txt
wait $!

rm -f $url_file

for src in $sources
do
    IFS=. read filename filext filerest <<< $src

    new_source="minimal_${filename}.${filext}"
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
