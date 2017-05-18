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

echo "Downloading redis . . ."
wget http://download.redis.io/releases/redis-2.6.17.tar.gz

echo "Unpacking redis tar . . ."
tar xzf redis-2.6.17.tar.gz

echo "Compiling redis. . . "
cd redis-2.6.17
make
cd ..

echo "Extracting redis binary and config. . . "
mv redis-2.6.17/src/redis-server .
mv redis-2.6.17/src/redis-cli .
mv redis-2.6.17/redis.conf .

echo "Cleaning up redis workspace . . ."
rm redis-2.6.17.tar.*
rm -rf redis-2.6.17

echo "Redis distribution ready"