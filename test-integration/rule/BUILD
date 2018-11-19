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

java_library(
    name = "grakn-test-server",
    srcs = ["GraknTestServer.java"],
    deps = [
        "//server",
        "//common",
        "//dependencies/maven/artifacts/junit:junit",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all",
        "//dependencies/maven/artifacts/org/apache/thrift:libthrift",
    ],
    data = [
        "//server:conf/grakn.properties",
        "//test-integration/resources:cassandra-embedded"
    ],
    visibility = ["//visibility:public"]
)