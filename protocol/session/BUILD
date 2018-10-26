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

exports_files([
    "Concept.proto",
    "Answer.proto",
    "Session.proto"
])

package(
    default_visibility = [
        "//protocol:__subpackages__",
        "//server:__subpackages__",
        "//client-java:__subpackages__",
        "//client-nodejs:__subpackages__",
        "//client_python:__subpackages__",
    ]
)

proto_library(
    name = "concept-proto",
    srcs = ["Concept.proto"],
    # proto_source_root = "protocol/session",
)

proto_library(
    name = "answer-proto",
    srcs = ["Answer.proto"],
    deps = [":concept-proto"],
    # proto_source_root = "protocol/session",
)

proto_library(
    name = "session-proto",
    srcs = ["Session.proto"],
    deps = [":concept-proto", ":answer-proto"],
    # proto_source_root = "protocol/session",
)