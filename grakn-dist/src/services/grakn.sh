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

export GRAKN_INCLUDE="${GRAKN_HOME}/services/grakn.in.sh"
. "$GRAKN_INCLUDE"

SLEEP_INTERVAL_S=2

clean_grakn() {
    echo -n "Are you sure you want to delete all stored data and logs? [y/N] " >&2
    read response
    if [ "$response" != "y" -a "$response" != "Y" ]; then
        echo "Response \"$response\" did not equal \"y\" or \"Y\".  Canceling clean operation." >&2
        return 0
    fi

    if cd "${GRAKN_HOME}/db"; then
        rm -rf cassandra es
        mkdir -p cassandra/data cassandra/commitlog cassandra/saved_caches es
        echo "Deleted data in `pwd`" >&2
        cd - >/dev/null
    else
        echo 'Data directory does not exist.' >&2
    fi

    if cd "${GRAKN_HOME}/logs"; then
        rm -f *.log
        echo "Deleted logs in `pwd`" >&2
        cd - >/dev/null
    fi

    #Clear redis and stop it
    "${GRAKN_HOME}/services/grakn-redis.sh" clean
}

case "$1" in

start)

    if ! "${GRAKN_HOME}/services/grakn-redis.sh" start ; then
      echo "Unable to start Redis!"
      exit 1
    fi
    if [ "$USE_CASSANDRA" ]; then
        "${GRAKN_HOME}/services/grakn-cassandra.sh" start
    fi
    "${GRAKN_HOME}/services/grakn-engine.sh" start
    ;;

stop)

    "${GRAKN_HOME}/services/grakn-redis.sh" stop
    "${GRAKN_HOME}/services/grakn-engine.sh" stop
    if [ "$USE_CASSANDRA" ]; then
        "${GRAKN_HOME}/services/grakn-cassandra.sh" stop
    fi
    ;;

clean)

    "${GRAKN_HOME}/services/grakn-engine.sh" stop
    if [ "$USE_CASSANDRA" ]; then
        "${GRAKN_HOME}/services/grakn-cassandra.sh" stop
        clean_grakn
    fi
    ;;

status)
    "${GRAKN_HOME}/services/grakn-redis.sh" status
    "${GRAKN_HOME}/services/grakn-engine.sh" status
    if [ "$USE_CASSANDRA" ]; then
        "${GRAKN_HOME}/services/grakn-cassandra.sh" status
    fi
    ;;

*)
    echo "Usage: $0 {start|stop|clean|status}"
    ;;

esac
