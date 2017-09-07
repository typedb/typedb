#!/bin/bash

# Force script to exit on failed command
set -e

SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`
GRAQL=${SCRIPTPATH}/graql
GRAQL_SCHMEA=${GRAQL}/schema
GRAQL_TEMPLATES=${GRAQL}/templates

echo "Loading Biomed Schema . . ."
graql.sh -k biomed -f ${GRAQL}/schema/schema.gql
graql.sh -k biomed -f ${GRAQL}/schema/rules.gql
graql.sh -k biomed -f ${GRAQL}/schema/new-rules.gql

echo "Loading Biomed Data . . ."
migration.sh csv -d -k biomed -t ${GRAQL_TEMPLATES}/hsa-mature-migrator.gql  -i data/hsa-mature.tsv
migration.sh csv -d -k biomed -t ${GRAQL_TEMPLATES}/hsa-hairpin-migrator.gql -i data/hsa-hairpin.tsv
migration.sh csv -d -k biomed -t ${GRAQL_TEMPLATES}/cancer-entities.gql -i data/entity_cancer.csv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/gene-entities.gql -i data/entity_gene.csv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/unique-cancer-mirna-relations.gql -i data/relation_mir_cancer.csv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/cancer-mirna-relations-migrator.gql -i data/miRCancerJune2016.tsv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/hsa-migrator-with-prec.gql -i data/hsa-with-prec.tsv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/hsa-migrator.gql -i data/hsa.tsv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/unique-mirna-gene-relations.gql -i data/relation_mir_gene.csv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/miRTarBase-migrator.gql -i data/miRTarBase_MTI_1000.tsv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/drugs-migrator.gql -i data/drugs.csv
migration.sh csv -d -k biomed -s \| -t ${GRAQL_TEMPLATES}/interactions-migrator.gql -i data/interactions.tsv

echo "Data load complete "
