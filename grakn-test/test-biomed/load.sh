#!/bin/bash

# Force script to exit on failed command
set -e

SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`

# Graql Directories
GRAQL=${SCRIPTPATH}/graql
GRAQL_SCHMEA=${GRAQL}/schema
GRAQL_TEMPLATES=${GRAQL}/templates
GRAQL_DATA=${GRAQL}/data

# Data Directory
DATA=${SCRIPTPATH}/data

if [ -d "${GRAQL_DATA}" ]; then
    echo "Downloading validation data . . . "
    git clone https://github.com/graknlabs/biomed.git
    mv biomed/data/ data/
    rm -rf biomed
fi

echo "Loading Biomed Schema . . ."
graql.sh -k biomed -f ${GRAQL}/schema/schema.gql

echo "Loading Biomed Rules . . ."
graql.sh -k biomed -f ${GRAQL}/schema/rules.gql

echo "Loading Biomed Data . . ."
migration.sh csv -d -k biomed -t ${GRAQL_TEMPLATES}/hsa-mature-migrator.gql  -i ${DATA}/hsa-mature.tsv
migration.sh csv -d -k biomed -t ${GRAQL_TEMPLATES}/hsa-hairpin-migrator.gql -i ${DATA}/hsa-hairpin.tsv
migration.sh csv -d -k biomed -t ${GRAQL_TEMPLATES}/cancer-entities.gql -i ${DATA}/entity_cancer.csv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/gene-entities.gql -i ${DATA}/entity_gene.csv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/unique-cancer-mirna-relations.gql -i ${DATA}/relation_mir_cancer.csv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/cancer-mirna-relations-migrator.gql -i ${DATA}/miRCancerJune2016.tsv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/hsa-migrator-with-prec.gql -i ${DATA}/hsa-with-prec.tsv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/hsa-migrator.gql -i ${DATA}/hsa.tsv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/unique-mirna-gene-relations.gql -i ${DATA}/relation_mir_gene.csv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/miRTarBase-migrator.gql -i ${DATA}/miRTarBase_MTI_1000.tsv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/drugs-migrator.gql -i ${DATA}/drugs.csv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/interactions-migrator.gql -i ${DATA}/interactions.tsv

# This is to compensate for the current failing state of analytics
echo "Loading Fake Degrees . . ."
graql.sh -k biomed -f ${GRAQL_DATA}/fake-degrees-schema.gql
echo "Adding fake interaction degrees . . ."
graql.sh -k biomed -f ${GRAQL_DATA}/fake-degrees-data-1.gql
echo "Adding fake Gene Target degrees . . ."
graql.sh -k biomed -f ${GRAQL_DATA}/fake-degrees-data-2.gql


echo "Data load complete "
