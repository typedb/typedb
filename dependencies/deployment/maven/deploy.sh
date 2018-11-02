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

#!/usr/bin/env bash


ARTIFACT="$ARTIFACT"
MAVEN_PASSWORD="$MAVEN_PASSWORD"
MAVEN_URL="$MAVEN_URL"
MAVEN_USERNAME="$MAVEN_USERNAME"
PARENT="$PARENT"
VERSION="{pom_version}"

curl -X PUT -u $MAVEN_USERNAME:$MAVEN_PASSWORD --upload-file pom.xml $MAVEN_URL/$PARENT/$VERSION/$ARTIFACT-$VERSION.pom
curl -X PUT -u $MAVEN_USERNAME:$MAVEN_PASSWORD --upload-file lib.jar $MAVEN_URL/$PARENT/$VERSION/$ARTIFACT-$VERSION.jar
