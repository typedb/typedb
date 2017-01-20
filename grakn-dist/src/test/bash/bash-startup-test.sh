#!/bin/bash

BASEDIR=$(dirname "$0")
GRAKN_DIST_DIR=$BASEDIR/../../../../grakn-dist/
GRAKN_TEST_TARGET=$BASEDIR/../../../target/
GRAKN_TEST_TMP=$GRAKN_TEST_TARGET/grakn/
GRAKN_TEST_BIN=${GRAKN_TEST_TMP}/bin/

startEngine(){
  echo "Starting!"
  "${GRAKN_TEST_BIN}/grakn.sh" start
  sleep 10
  echo "exit" | "${GRAKN_TEST_BIN}"/graql.sh # Waiting for Engine
}

loadData(){
  echo "Inserting data!"
  "${GRAKN_TEST_BIN}"/graql.sh < insert-data.gql
}

oneTimeSetUp() {
  set -e
  package=$(ls $GRAKN_DIST_DIR/target/grakn-dist-*.tar.gz | head -1)
  cp "$package" $GRAKN_TEST_TARGET/
  mkdir -p ${GRAKN_TEST_TMP}
  tar -xf $GRAKN_TEST_TARGET/$(basename ${package}) -C ${GRAKN_TEST_TMP} --strip-components=1

  startEngine
  loadData
  set +e
}

oneTimeTearDown() {
  echo "y" | "${GRAKN_TEST_BIN}"/grakn.sh clean
  "${GRAKN_TEST_BIN}"/grakn.sh stop
}

testPersonCount()
{
  PERSON_COUNT=$(cat query-data.gql | "${GRAKN_TEST_BIN}"/graql.sh | grep -v '>>>' | grep person | wc -l)
  echo Persons found $PERSON_COUNT
  assertEquals 4 $PERSON_COUNT
}

testMarriageCount()
{
  MARRIAGE_COUNT=$(cat query-marriage.gql | "${GRAKN_TEST_BIN}"/graql.sh |  grep -v '>>>' | grep person | wc -l)
  echo Marriages found $MARRIAGE_COUNT
  assertEquals 1 $MARRIAGE_COUNT
}

# Source shunit2
source "$GRAKN_TEST_TARGET"/lib/shunit2-master/source/2.1/src/shunit2
