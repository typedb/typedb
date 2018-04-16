#!/bin/bash

###
# #%L
# test-snb
# %%
# Copyright (C) 2016 - 2018 Grakn Labs Ltd
# %%
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
# #L%
###

source snb-env.sh

LDBC_VALIDATION_CONFIG=${WORKSPACE}/grakn-test/test-snb/src/main/bash/readwrite_grakn--ldbc_driver_config--db_validation.properties

# execute validation
mvn install --batch-mode -DskipTests --also-make --projects grakn-test/test-snb
mvn exec:java --batch-mode --projects grakn-test/test-snb \
    -Dexec.mainClass=com.ldbc.driver.Client \
    -Dlogback.configurationFile=${WORKSPACE}/conf/test/logback-test.xml \
    -Dexec.args="
    -db ai.grakn.GraknDb \
    -P ${LDBC_VALIDATION_CONFIG} \
    -vdb ${CSV_DATA}/validation_params.csv \
    -p ldbc.snb.interactive.parameters_dir ${CSV_DATA} \
    -p ai.grakn.uri ${ENGINE_REST} \
    -p ai.grakn.keyspace ${KEYSPACE}"

# check for errors from LDBC
FAILURES=$(cat ${CSV_DATA}/validation_params-failed-actual.json)
EXPECTED=$(cat ${CSV_DATA}/validation_params-failed-expected.json)
if [ "${FAILURES}" == "[ ]" ]; then
        echo "Validation completed without failures."
else
        echo "There were failures during validation."
        echo "Actual:"
        echo ${FAILURES}
        echo "Expected:"
        echo ${EXPECTED}
        exit 1
fi
