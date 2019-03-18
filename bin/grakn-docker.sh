#!/usr/bin/env bash
#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

function cleanup() {
    echo 'Caught an exit signal'
    trap - SIGINT SIGTERM
    kill $(pidof tail)
    ./grakn server stop
    exit
}

trap cleanup SIGINT SIGTERM

pushd grakn-core-all-linux &>/dev/null
./grakn server start
tail -f logs/grakn.log &

while sleep 60; do
  jps | grep -q Grakn$
  GRAKN_STATUS=$?
  jps | grep -q GraknStorage$
  GRAKN_STORAGE_STATUS=$?
  # If the greps above find anything, they exit with 0 status
  # If they are not both 0, then something is wrong
  if [ $GRAKN_STATUS -ne 0 -o $GRAKN_STORAGE_STATUS -ne 0 ]; then
    echo "One of the processes (Server/Storage) has already exited."
    exit 1
  fi
done
