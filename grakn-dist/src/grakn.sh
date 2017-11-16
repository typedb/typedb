#!/bin/bash

#
# Grakn - A Distributed Semantic Database
# Copyright (C) 2016  Grakn Labs Limited
#
# Grakn is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Grakn is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# <http://www.gnu.org/licenses/gpl.txt>.

# ================================================
# grakn
# this script is divided into several major parts
# global variables
# common helper functions
# storage component helper functions - storage_*
# queue component helper functions - queue_*
# grakn helper functions - grakn_*
# command line helpers - cli_*
# ================================================

# ================================================
# common helper functions
# ================================================
update_classpath_global_var() {
  # Define CLASSPATH, exclude slf4j as we use logback
  for jar in ./services/lib/*.jar; do
      if [[ $jar != *slf4j-log4j12* ]] ; then
          CLASSPATH="$CLASSPATH":"$jar"
      fi
  done

  # Add path containing grakn.properties and logback.xml
  CLASSPATH="$CLASSPATH":./conf
  CLASSPATH="$CLASSPATH":./services/grakn
}

print_failure_diagnostics() {
  echo "======== Failure Diagnostics ========"
  echo "Grakn pid = '`cat $GRAKN_PID`' (from $GRAKN_PID), '`grakn_get_pid_by_ps_ef`' (from ps -ef)"
  echo "Queue pid = '`cat $QUEUE_PID`' (from $QUEUE_PID), '`queue_get_pid_by_ps_ef`' (from ps -ef)"
  echo "Storage pid = '`cat $STORAGE_PID`' (from $STORAGE_PID)"
}

print_failure_diagnostics() {
  echo "======== Failure Diagnostics ========"
  echo "Grakn pid = '`cat $GRAKN_PID`' (from $GRAKN_PID), '`grakn_get_pid_by_ps_ef`' (from ps -ef)"
  echo "Queue pid = '`cat $QUEUE_PID`' (from $QUEUE_PID), '`queue_get_pid_by_ps_ef`' (from ps -ef)"
  echo "Storage pid = '`cat $STORAGE_PID`' (from $STORAGE_PID)"
}


# =============================================
# misc helper methods
# =============================================
print_grakn_logo() {
  # cat ASCII logo, or fail silently if it's somehow missing
  cat ./services/grakn/grakn-ascii.txt 2> /dev/null
}

# =============================================
# main routine
# =============================================


# Grakn global variables
# TODO: clean up
if [ -z "${GRAKN_HOME}" ]; then
    [[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
    GRAKN_HOME=$(cd "$(dirname "${path}")" && pwd -P)
fi


pushd "$GRAKN_HOME" > /dev/null

update_classpath_global_var

# temporarily disabling trap as it's probably not needed
# trap handle_ctrl_c INT # run handle_ctrl_c when receiving CTRL+C

print_grakn_logo

case "$1" in
  server)
    case "$2" in
      start)
        java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.dist.DistApplication ${GRAKN_HOME} $1 $2 $3
      ;;
      stop)
        java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.dist.DistApplication ${GRAKN_HOME} $1 $2 $3
      ;;
      status)
        java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.dist.DistApplication ${GRAKN_HOME} $1 $2 $3
      ;;
      clean)
        cli_case_grakn_server_clean
      ;;
      help|*)
        java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.dist.DistApplication ${GRAKN_HOME} $1 $2 $3
      ;;
    esac
  ;;
  version)
    java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.dist.DistApplication ${GRAKN_HOME} $1 $2 $3
  ;;
  *|help)
    java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.dist.DistApplication ${GRAKN_HOME} $1 $2 $3
  ;;
esac

popd > /dev/null
