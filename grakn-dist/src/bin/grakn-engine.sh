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

ENGINE_STARTUP_TIMEOUT_S=30
SLEEP_INTERVAL_S=2

ENGINE_PS=/tmp/grakn-engine.pid

# Define CLASSPATH, exclude slf4j as we use logback
for jar in "${GRAKN_HOME}"/lib/*.jar; do
    if [[ $jar != *slf4j-log4j12* ]] ; then
        CLASSPATH="$CLASSPATH":"$jar"
    fi
done

case "$1" in

start)

    if [ -e "$ENGINE_PS" ] && ps -p `cat $ENGINE_PS` > /dev/null ; then
        echo "Engine already running"
    else
        # engine has not already started
        echo -n "Starting engine"
        if [[ "$FOREGROUND" = true ]]; then
            java -cp "${CLASSPATH}" -Dgrakn.dir="${GRAKN_HOME}/bin" -Dgrakn.conf="${GRAKN_CONFIG}" ai.grakn.engine.GraknEngineServer
        else
            java -cp "${CLASSPATH}" -Dgrakn.dir="${GRAKN_HOME}/bin" -Dgrakn.conf="${GRAKN_CONFIG}" ai.grakn.engine.GraknEngineServer &
            echo $!>$ENGINE_PS
        fi
    fi
    ;;

stop)

    echo "Stopping engine"
    if [[ -e "$ENGINE_PS" ]]; then
        kill `cat $ENGINE_PS`
        rm $ENGINE_PS
    fi
    ;;

status)

    ENGINE_PIDS=$(ps ax | grep -i 'ai\.grakn\.engine\.GraknEngineServer' | grep java | grep -v grep | awk '{print $1}')
    if [ -e "$ENGINE_PS" ] && ps -p `cat $ENGINE_PS` > /dev/null ; then
        echo "Engine is $(cat $ENGINE_PS)"
    elif [ -n "$ENGINE_PIDS" ]; then
        echo "Engine is $ENGINE_PIDS (foreground)"
    else
        echo "Engine has stopped"
    fi
    ;;

*)
    echo "Usage: $0 {start|stop|clean|status}"
    ;;

esac
