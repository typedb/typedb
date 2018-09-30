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
    name = "session-proto",
    srcs = ["proto/Session.proto"],
    deps = [":answer-proto",
            ":concept-proto"],
    proto_source_root = "protocol",
    visibility = ["//client-protocol:__subpackages__"]
)

proto_library(
    name = "answer-proto",
    srcs = ["proto/Answer.proto"],
    deps = [":concept-proto"],
    proto_source_root = "protocol",
    visibility = ["//client-protocol:__subpackages__"]
)

proto_library(
    name = "concept-proto",
    srcs = ["proto/Concept.proto"],
    proto_source_root = "protocol",
    visibility = ["//client-protocol:__subpackages__"]
)

proto_library(
    name = "keyspace-proto",
    srcs = ["proto/Keyspace.proto"],
    proto_source_root = "protocol",
    visibility = ["//client-protocol:__subpackages__"]
)


java_proto_library(
    name = "client-java-proto",
    deps = [":session-proto",
            ":answer-proto",
            "concept-proto",
            "keyspace-proto"],
    visibility = [
        "//server:__subpackages__",
        "//client-java:__subpackages__",
    ]
)

load("@com_google_protobuf//:protobuf.bzl", "py_proto_library")
py_proto_library(
    name = "client-python-proto",
    srcs = glob(["proto/*.proto"]),
    protoc = "@com_google_protobuf//:protoc",
    visibility = [
        "//client-python:__subpackages__",
    ]
)