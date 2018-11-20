#!/usr/bin/env bash

# Download data required for Open Targets from the Ensembl public MySQL server
# Requires the mysql command-line client to be installed
# Takes one argument which is the file to which the Ensembl data should be downloaded

DATABASE=homo_sapiens_core_93_38

OUTPUT=$1

echo "Downloading from ${DATABASE} to ${OUTPUT}"

read -r -d '' SQL<<EOF 
SELECT  g.biotype,
        g.description,
        g.seq_region_start AS start,
        g.seq_region_end AS end,
        g.seq_region_strand AS strand,
        g.version,
        g.source,
        sr.name AS seq_region_name,
        g.stable_id AS id,
        (SELECT meta_value FROM meta WHERE meta_key='assembly.default') AS assembly_name, 
        (SELECT meta_value FROM meta WHERE meta_key='schema_type') AS db_type, 
        (SELECT meta_value FROM meta WHERE meta_key='species.production_name') AS species, 
        (SELECT meta_value FROM meta WHERE meta_key='schema_version') AS ensembl_release, 
        IF (sr.name IN ('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','X','Y','MT'), 'true', 'false') AS is_reference, 
        x.display_label AS display_name,
        'Gene' AS object_type,
        a.logic_name
FROM    gene g, seq_region sr, xref x, analysis a
WHERE   g.seq_region_id = sr.seq_region_id AND g.display_xref_id=x.xref_id AND g.analysis_id=a.analysis_id
EOF

mysql --compress --batch --host=ensembldb.ensembl.org --user=anonymous --execute="${SQL}" ${DATABASE} | gzip > ${OUTPUT}

echo "${OUTPUT} created with" `gzcat ${OUTPUT} | wc -l | tr -s ' ' | cut -d ' ' -f 2` "rows"