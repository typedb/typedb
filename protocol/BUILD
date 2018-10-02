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

proto_library(
    name = "client-proto",
    srcs = glob(["proto/*.proto"]),
    visibility = ["//protocol:__subpackages__"]
)

load("@org_pubref_rules_proto//java:compile.bzl", "java_grpc_compile")
java_grpc_compile(
    name = "client-java-proto",
    deps = [":client-proto"],
)

load("@org_pubref_rules_proto//python:compile.bzl", "python_grpc_compile")
python_grpc_compile(
    name = "client-python-proto",
    deps = [":client-proto"],
)

load("@org_pubref_rules_proto//node:compile.bzl", "node_grpc_compile")
node_grpc_compile(
    name = "client-node-proto",
    deps = [":client-proto"],
)
