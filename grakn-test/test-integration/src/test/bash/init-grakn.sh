#!/usr/bin/env bash

###
# #%L
# test-integration
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

source env.sh

if [ "$#" -gt "0" ]; then
    echo "Using branch name ${1}"
    DIST=grakn-dist/target/grakn-dist-${1}.tar.gz
else
    # get first thing matching wildcard pattern
    DIST=(grakn-dist/target/grakn-dist*.tar.gz)
    DIST=${DIST[0]}
fi

if [ -d "${PACKAGE}" ] ;  then rm -rf ${PACKAGE} ; fi

mkdir ${PACKAGE}

tar -xf ${DIST} --strip=1 -C ${PACKAGE}

# set DEBUG log level
# TODO: support this in a more elegant way (command line arg?)
sed -i'' -e 's/log.level=INFO/log.level=DEBUG/g' "${PACKAGE}/conf/grakn.properties"

grakn server start
