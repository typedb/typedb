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

REDIS_STARTUP_TIMEOUT_S=10
SLEEP_INTERVAL_S=2
REDIS_PS=/tmp/grakn-redis.pid

if [ -z "${GRAKN_HOME}" ]; then
    [[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
    GRAKN_BIN=$(cd "$(dirname "${path}")" && pwd -P)
    GRAKN_HOME=$(cd "${GRAKN_BIN}"/.. && pwd -P)
fi

redisRunning()
{
    echo $(ps -ef | grep "redis-server" | grep -v grep | awk '{ print $2}')
}

waitForRedis() {
    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $REDIS_STARTUP_TIMEOUT_S ))

    while [ $now_s -le $stop_s ]; do
        echo -n .
        # The \r\n deletion bit is necessary for Cygwin compatibility
        if [ $(redisRunning) ]; then
            echo
            return 0
        fi
        sleep $SLEEP_INTERVAL_S
        now_s=`date '+%s'`
    done

    echo " timeout exceeded ($REDIS_STARTUP_TIMEOUT_S seconds)" >&2
    return 1
}

executeRedisServer(){
    if [ "$(uname)" == "Darwin" ]; then
        "${GRAKN_HOME}/services/redis/"redis-server-osx $1
    elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
        "${GRAKN_HOME}/services/redis/"redis-server-linux $1
    fi
}

executeRedisCli(){
    if [ "$(uname)" == "Darwin" ]; then
        "${GRAKN_HOME}/services/redis/"redis-cli-osx $1
    elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
        "${GRAKN_HOME}/services/redis/"redis-cli-linux $1
    fi
}

case "$1" in
start)
    if [ $(redisRunning) ] ; then
        echo "Redis is already running"
    else
        echo "Starting Redis"
        if ! executeRedisServer "${GRAKN_HOME}/services/redis/redis.conf" ; then exit 1 ; fi
        if ! waitForRedis ; then exit 1 ; fi
    fi
    ;;
stop)
    echo "Stopping Redis"
    executeRedisCli shutdown
    ;;
clean)
    echo "Cleaning Redis"

    if [ ! $(redisRunning) ] ; then
        executeRedisServer "${GRAKN_HOME}/services/redis/redis.conf"
    fi

    executeRedisCli flushall
    executeRedisCli shutdown
    ;;
status)
    if [ -e $REDIS_PS ] && ps -p `cat $REDIS_PS` > /dev/null ; then
        echo "Redis is running"
    else
        echo "Redis has stopped"
    fi
    ;;
esac
