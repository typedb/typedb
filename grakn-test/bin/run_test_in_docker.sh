#!/bin/bash
# Requires the test profile as argument, ie. tinker, titan

if [[ -z "$1" ]]; then
  echo Test profile required. eg. tinker/titan
  exit 1
fi  

SCRIPT_DIR=$(dirname $0)
GRAKN_TEST_PROFILE=$1

# Find all tests in grakn-test
find "$SCRIPT_DIR"/../../grakn-test/ -name '*Test*.java' -o -name '*IT*.java' | xargs --no-run-if-empty -n1 basename | sed -e 's/\.java//' > /tmp/grakn_mvn_docker_test_list

# Run parallel at '75% core capacity'
# Mount volume so tests results are output in surefire-reports
# Picks a tests from the list above and starts container to run test within grakn-test project

parallel --jobs 75% \
  --no-run-if-empty \
  "/usr/bin/docker run -i \
  -v "$SCRIPT_DIR"/../../grakn-test/target/surefire-reports:/grakn-src/grakn-test/target/surefire-reports/ \
  -w /grakn-src/ graknlabs/jenkins-with-src-compiled:latest \
  mvn test -DfailIfNoTests=false \
  -Dtest={} \
  -P"${GRAKN_TEST_PROFILE}" \
  -pl grakn-test" \
  < /tmp/grakn_mvn_docker_test_list
