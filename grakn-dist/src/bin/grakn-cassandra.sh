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

if [ -z "${GRAKN_HOME}" ]; then
    [[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
    GRAKN_BIN=$(cd "$(dirname "${path}")" && pwd -P)
    GRAKN_HOME=$(cd "${GRAKN_BIN}"/.. && pwd -P)
fi

export GRAKN_INCLUDE="${GRAKN_HOME}/bin/grakn.in.sh"
. "$GRAKN_INCLUDE"

CASSANDRA_STARTUP_TIMEOUT_S=60
SLEEP_INTERVAL_S=2
NODETOOL="${GRAKN_HOME}/bin/nodetool"

CASSANDRA_PS=/tmp/grakn-cassandra.pid

# from titan
wait_for_cassandra() {
    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $CASSANDRA_STARTUP_TIMEOUT_S ))
    local status_thrift=

    while [ $now_s -le $stop_s ]; do
        echo -n .
        # The \r\n deletion bit is necessary for Cygwin compatibility
        status_thrift="`"$NODETOOL" statusthrift 2>/dev/null | tr -d '\n\r'`"
        if [ $? -eq 0 -a 'running' = "$status_thrift" ]; then
            echo
            return 0
        fi
        sleep $SLEEP_INTERVAL_S
        now_s=`date '+%s'`
    done

    echo " timeout exceeded ($CASSANDRA_STARTUP_TIMEOUT_S seconds)" >&2
    return 1
}

case "$1" in

start)

    if [ -e $CASSANDRA_PS ] && ps -p `cat $CASSANDRA_PS` > /dev/null ; then
        echo "Cassandra already running"
    else
        # cassandra has not already started
        echo -n "Starting cassandra "
        echo ""
        "${GRAKN_HOME}"/bin/cassandra -p $CASSANDRA_PS

        if ! wait_for_cassandra ; then exit 1 ; fi
    fi
    ;;

stop)

    echo "Stopping cassandra"
    if [[ -e $CASSANDRA_PS ]]; then
        pid=`cat $CASSANDRA_PS`
        kill "$pid"
        while kill -0 "$pid" 2>/dev/null; do
            sleep 0.5
        done
    fi
    ;;

status)

    if [ -e $CASSANDRA_PS ] && ps -p `cat $CASSANDRA_PS` > /dev/null ; then
        echo "Cassandra is running"
    else
        echo "Cassandra has stopped"
    fi
    ;;

*)
    echo "Usage: $0 {start|stop|status}"
    ;;

esac
