#!/bin/bash

# set script directory as working directory
SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`
DATA=$SCRIPTPATH/./social_network
GRAQL=$SCRIPTPATH/./graql

# generate CSV files
$SCRIPTPATH/run.sh

exit
# load ontology
graql.sh -k $2 -f $GRAQL/ldbc-snb-1-resources.gql -r $1
graql.sh -k $2 -f $GRAQL/ldbc-snb-2-relations.gql -r $1
graql.sh -k $2 -f $GRAQL/ldbc-snb-3-entities.gql -r $1
graql.sh -k $2 -f $GRAQL/ldbc-snb-4-rules.gql -r $1

sed -i '' "1s/Comment.id|Comment.id/Comment.id|Message.id/" $DATA/comment_replyOf_comment_0_0.csv
sed -i '' "1s/Person.id|Person.id/Person1.id|Person.id/" $DATA/person_knows_person_0_0.csv
sed -i '' "1s/Place.id|Place.id/Place1.id|Place.id/" $DATA/place_isPartOf_place_0_0.csv
sed -i '' "1s/TagClass.id|TagClass.id/TagClass1.id|TagClass.id/" $DATA/tagclass_isSubclassOf_tagclass_0_0.csv

while read p;
do
        DATA_FILE=$(echo $p | awk '{print $2}')
        TEMPLATE_FILE=$(echo $p | awk '{print $1}')

        NUM_SPLIT=$(head -1 ${DATA}/${DATA_FILE} | tr -cd \| | wc -c)
        BATCH_SIZE=$(awk "BEGIN {print int(1000/${NUM_SPLIT})}")

        echo $BATCH_SIZE

        tail -n +2 $DATA/${DATA_FILE} | wc -l
        time migration.sh csv -s \| -t $GRAQL/${TEMPLATE_FILE} -i $DATA/${DATA_FILE} -k $2 -u $1 -a ${3:-25} -b ${BATCH_SIZE}
done < migrationsToRun.txt
