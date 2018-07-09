#!/usr/bin/env bash

source snb-env.sh

# set script directory as working directory
SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`

GRAQL=${SCRIPTPATH}/../graql

ACTIVE_TASKS=16

# validate the number of arguments
if [ "$#" -lt "2" ]; then
    echo "Wrong number of arguments." >&2
    exit 1
fi

# extract the data from a tar
function extractArchData {

    if [ -z ${CSV_DATA+x} ]; then
        echo ${CSV_DATA}
        echo "Environment Variable Not Set. Please run 'source local-env.sh'"
        exit 1
    fi

    mkdir -p ${CSV_DATA}
    case "$1" in
        validate)
            VALIDATION_DATA=${WORKSPACE}/${PACKAGE}/validation_set.tar.gz
            wget https://github.com/ldbc/ldbc_snb_interactive_validation/raw/master/neo4j/readwrite_neo4j--validation_set.tar.gz -O ${VALIDATION_DATA}
            tar -xf ${VALIDATION_DATA} --strip=1 -C ${CSV_DATA} validation_set
            ;;
        SF1)
            tar -xf ${SF1_DATA}
            ;;
        *)
            echo "Usage: arch {validate|SF1}"
            exit 1
            ;;
    esac
}

# generate new data
function generateData {

    if [ -z ${HADOOP_HOME+x} ]; then
        echo '$HADOOP_HOME not set'
        exit 1
    fi

    if [ -z ${LDBC_SNB_DATAGEN_HOME+x} ]; then
        echo '$LDBC_SNB_DATAGEN_HOME not set'
        exit 1
    fi

    paramFile=${SCRIPTPATH}/tmpParams.ini
    cp ${LDBC_SNB_DATAGEN_HOME}/params.ini ${paramFile}

    case "$1" in
        SF*)

            echo "ldbc.snb.datagen.generator.scaleFactor:snb.interactive.${1:2:4}" >> ${paramFile}
            ;;
        P*)
            echo "ldbc.snb.datagen.generator.numPersons:${1:1:6}" >> ${paramFile}
            ;;
        *)
            echo "Usage: gen {SF*|P*}"
            exit 1
            ;;
    esac

    export HADOOP_CLIENT_OPTS="-Xmx1024m"
    LDBC_JAR=${LDBC_SNB_DATAGEN_HOME}/target/ldbc_snb_datagen-0.2.7-jar-with-dependencies.jar

    # The jar contains both a folder called `META-INF/license` and a file `META-INF/LICENSE`.
    # This causes issues when Hadoop unzips it on a case-insensitive file system such as OSX.
    # https://stackoverflow.com/questions/10522835/hadoop-java-io-ioexception-mkdirs-failed-to-create-some-path
    zip -d ${LDBC_JAR} META-INF/LICENSE || true

    ${HADOOP_HOME}/bin/hadoop jar ${LDBC_JAR} ${paramFile}

    rm ${paramFile}
    rm -f m*personFactors*
    rm -f .m*personFactors*
    rm -f m*activityFactors*
    rm -f .m*activityFactors*
    rm -f m0friendList*
    rm -f .m0friendList*
}

# switch between generating data or using archive data
case "$1" in
    gen)
        generateData $2
        ;;
    arch)
        extractArchData $2
        ;;
    *)
        echo "Usage: $0 {gen|arch}"
        exit 1
        ;;
esac

# migrate the data into Grakn

graql console -k ${KEYSPACE} -f ${GRAQL}/ldbc-snb-1-resources.gql -r ${ENGINE_GRPC}
graql console -k ${KEYSPACE} -f ${GRAQL}/ldbc-snb-2-relations.gql -r ${ENGINE_GRPC}
graql console -k ${KEYSPACE} -f ${GRAQL}/ldbc-snb-3-entities.gql -r ${ENGINE_GRPC}
graql console -k ${KEYSPACE} -f ${GRAQL}/ldbc-snb-4-rules.gql -r ${ENGINE_GRPC}

# lazily take account of OS
unamestr=`uname`
if [[ "$unamestr" == 'Linux' ]]; then
    sed -i "1s/Comment.id|Comment.id/Comment.id|Message.id/" ${CSV_DATA}/comment_replyOf_comment_0_0.csv
    sed -i "1s/Person.id|Person.id/Person1.id|Person.id/" ${CSV_DATA}/person_knows_person_0_0.csv
    sed -i "1s/Place.id|Place.id/Place1.id|Place.id/" ${CSV_DATA}/place_isPartOf_place_0_0.csv
    sed -i "1s/TagClass.id|TagClass.id/TagClass1.id|TagClass.id/" ${CSV_DATA}/tagclass_isSubclassOf_tagclass_0_0.csv
elif [[ "$unamestr" == 'Darwin' ]]; then
    sed -i '' "1s/Comment.id|Comment.id/Comment.id|Message.id/" ${CSV_DATA}/comment_replyOf_comment_0_0.csv
    sed -i '' "1s/Person.id|Person.id/Person1.id|Person.id/" ${CSV_DATA}/person_knows_person_0_0.csv
    sed -i '' "1s/Place.id|Place.id/Place1.id|Place.id/" ${CSV_DATA}/place_isPartOf_place_0_0.csv
    sed -i '' "1s/TagClass.id|TagClass.id/TagClass1.id|TagClass.id/" ${CSV_DATA}/tagclass_isSubclassOf_tagclass_0_0.csv
fi

while read p;
do
        DATA_FILE=$(echo $p | awk '{print $2}')
        TEMPLATE_FILE=$(echo $p | awk '{print $1}')

        echo "Loading ${DATA_FILE} with ${TEMPLATE_FILE}"

        tail -n +2 $CSV_DATA/${DATA_FILE} | wc -l
        echo graql migrate csv -s \| -t ${GRAQL}/${TEMPLATE_FILE} -i ${CSV_DATA}/${DATA_FILE} -r 5 -k ${KEYSPACE} -u ${ENGINE_REST} -d
        time graql migrate csv -s \| -t ${GRAQL}/${TEMPLATE_FILE} -i ${CSV_DATA}/${DATA_FILE} -r 5 -k ${KEYSPACE} -u ${ENGINE_REST} -d
done < ${SCRIPTPATH}/migrationsToRun.txt
