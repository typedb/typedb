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
migration.sh csv -d -k biomed -t migrators/hsa-mature-migrator.gql  -i data/hsa-mature.tsv
migration.sh csv -d -k biomed -t migrators/hsa-hairpin-migrator.gql -i data/hsa-hairpin.tsv
migration.sh csv -d -k biomed -t migrators/cancer-entities.gql -i data/entity_cancer.csv
migration.sh csv -d -k biomed -s \| -t migrators/gene-entities.gql -i data/entity_gene.csv
migration.sh csv -d -k biomed -s \| -t migrators/unique-cancer-mirna-relations.gql -i data/relation_mir_cancer.csv
migration.sh csv -d -k biomed -s \| -t migrators/cancer-mirna-relations-migrator.gql -i data/miRCancerJune2016.tsv
migration.sh csv -d -k biomed -s \| -t migrators/hsa-migrator-with-prec.gql -i data/hsa-with-prec.tsv
migration.sh csv -d -k biomed -s \| -t migrators/hsa-migrator.gql -i data/hsa.tsv
migration.sh csv -d -k biomed -s \| -t migrators/unique-mirna-gene-relations.gql -i data/relation_mir_gene.csv
migration.sh csv -d -k biomed -s \| -t migrators/miRTarBase-migrator.gql -i data/miRTarBase_MTI_1000.tsv
migration.sh csv -d -k biomed -s \| -t migrators/drugs-migrator.gql -i data/drugs.csv
migration.sh csv -d -k biomed -s \| -t migrators/interactions-migrator.gql -i data/interactions.tsv

echo "Data load complete "
