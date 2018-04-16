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

if [ -d maven ] ;  then rm -rf maven ; fi

if pgrep -l redis-server ; then
    echo "WARNING: Redis is still running at tear down - killing process"
    pkill -9 redis-server
fi
