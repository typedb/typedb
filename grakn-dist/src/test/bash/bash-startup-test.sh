#!/bin/bash

BASEDIR=$(dirname "$0")
GRAKN_DIST_TARGET=$BASEDIR/../../../target/
GRAKN_DIST_TMP=$GRAKN_DIST_TARGET/grakn-bash-test/
GRAKN_DIST_BIN=${GRAKN_DIST_TMP}/bin/

startEngine(){
  echo "Starting!"
  _JAVA_OPTIONS="-Dcassandra.log.appender=STDOUT" "${GRAKN_DIST_BIN}/grakn.sh" start
}

loadData(){
  echo "Inserting data!"
  "${GRAKN_DIST_BIN}"/graql.sh < insert-data.gql
}

oneTimeSetUp() {
  set -e
  package=$(ls $GRAKN_DIST_TARGET/grakn-dist-*.tar.gz | head -1)
  mkdir -p ${GRAKN_DIST_TMP}
  tar -xf $GRAKN_DIST_TARGET/$(basename ${package}) -C ${GRAKN_DIST_TMP} --strip-components=1

  startEngine
  loadData
  set +e
}

oneTimeTearDown() {
  echo "y" | "${GRAKN_DIST_BIN}"/grakn.sh clean
  "${GRAKN_DIST_BIN}"/grakn.sh stop
}

testPersonCount()
{
  PERSON_COUNT=$(cat query-data.gql | "${GRAKN_DIST_BIN}"/graql.sh | grep -v '>>>' | grep person | wc -l)
  echo Persons found $PERSON_COUNT
  assertEquals 4 $PERSON_COUNT
}

testMarriageCount()
{
  MARRIAGE_COUNT=$(cat query-marriage.gql | "${GRAKN_DIST_BIN}"/graql.sh |  grep -v '>>>' | grep person | wc -l)
  echo Marriages found $MARRIAGE_COUNT
  assertEquals 1 $MARRIAGE_COUNT
}

# Source shunit2
source "$GRAKN_DIST_TARGET"/lib/shunit2-master/source/2.1/src/shunit2
