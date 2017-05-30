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

set -e

echo "Downloading redis . . ."
wget https://github.com/fppt/redis-prebuilt/raw/master/redis-cli-linux
wget https://github.com/fppt/redis-prebuilt/raw/master/redis-cli-osx
wget https://github.com/fppt/redis-prebuilt/raw/master/redis-server-linux
wget https://github.com/fppt/redis-prebuilt/raw/master/redis-server-osx
chmod +x redis-*
echo "Redis distribution ready"