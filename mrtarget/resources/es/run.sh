
#index                       pri rep docs.count docs.deleted
#19.04_association-data        9   0    7812201            0
#19.04_eco-data                9   0       3764            0
#19.04_efo-data                9   0      14580            0
#19.04_ensembl-data            5   1      58336            0
#19.04_evidence-data           9   0   12397645            0
#19.04_expression-data         9   0      50277            0
#19.04_gene-data               9   0      58336            0
#19.04_invalid-evidence-data   1   0    2538434            0
#19.04_reactome-data           5   1       2255            0
#19.04_relation-data           9   0   38727618            0
#19.04_search-data             9   0      72916            0
#19.04_uniprot-data            9   0      20417            0


curl -s 'localhost:9201/19.04_reactome-data/_mapping' | jq '.["19.04_reactome-data"].mappings' > rea_mappings.json
curl -s 'localhost:9201/19.04_ensembl-data/_mapping' | jq '.["19.04_ensembl-data"].mappings' > ens_mappings.json
curl -s 'localhost:9201/19.04_uniprot-data/_mapping' | jq '.["19.04_uniprot-data"].mappings' > uni_mappings.json
curl -s 'localhost:9201/19.04_efo-data/_mapping' | jq '.["19.04_efo-data"].mappings' > efo_mappings.json
curl -s 'localhost:9201/19.04_eco-data/_mapping' | jq '.["19.04_eco-data"].mappings' > eco_mappings.json
curl -s 'localhost:9201/19.04_expression-data/_mapping' | jq '.["19.04_expression-data"].mappings' > hpa_mappings.json
curl -s 'localhost:9201/19.04_gene-data/_mapping' | jq '.["19.04_gene-data"].mappings' > gen_mappings.json
curl -s 'localhost:9201/19.04_evidence-data/_mapping' | jq '.["19.04_evidence-data"].mappings' > val_right_mappings.json
curl -s 'localhost:9201/19.04_invalid-evidence-data/_mapping' | jq '.["19.04_invalid-evidence-data"].mappings' > val_wrong_mappings.json
curl -s 'localhost:9201/19.04_association-data/_mapping' | jq '.["19.04_association-data"].mappings' > as_mappings.json
curl -s 'localhost:9201/19.04_search-data/_mapping' | jq '.["19.04_search-data"].mappings' > sea_mappings.json
curl -s 'localhost:9201/19.04_relation-data/_mapping' | jq '.["19.04_relation-data"].mappings' > ddr_mappings.json
