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

workspace(name = "grakn_core")

# Load additional build tools, such bazel-deps and unused-deps
load("//dependencies/tools:dependencies.bzl", "tools_dependencies")
tools_dependencies()

# Load runtime dependencies from maven
load("//dependencies/maven:dependencies.bzl", "maven_dependencies")
maven_dependencies()

# Load additional compiler tools, such as ANTLR and Protobuf
load(
    "//dependencies/compilers:dependencies.bzl",
    "antlr_dependencies",
    "protobuf_dependencies"
)
antlr_dependencies()
protobuf_dependencies()

# Load dependencies for ANTLR rules
load("@rules_antlr//antlr:deps.bzl", "antlr_dependencies")
antlr_dependencies()

# Load versions rule from skylib, for Protobuf, and check that bazel >= 0.5.4
load("@bazel_skylib//:lib.bzl", "versions")
versions.check(minimum_bazel_version = "0.5.4")

