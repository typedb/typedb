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

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def graknlabs_graql():
     git_repository(
         name = "graknlabs_graql",
         remote = "https://github.com/graknlabs/graql",
         commit = "e452fd0eee312264eea6255f94ce7c46d805d04c",
     )

def graknlabs_client_java():
     git_repository(
         name = "graknlabs_client_java",
         remote = "https://github.com/graknlabs/client-java",
         commit = "e6fc5d0c9a5d998dddb0d0c7bc50358bd9d6428e",
     )

def graknlabs_benchmark():
    git_repository(
        name = "graknlabs_benchmark",
        remote = "https://github.com/graknlabs/benchmark.git",
        commit = "ceb5a2ebb71ee526d788fb4b17a104a6669d4b70" # keep in sync with protocol changes
    )

def graknlabs_build_tools():
    # TODO: revert to graknlabs
#    native.local_repository(
#            name = "graknlabs_build_tools",
#            path = "/Users/lolski/grakn.ai/build-tools"
#        )
    git_repository(
        name = "graknlabs_build_tools",
        remote = "https://github.com/lolski/build-tools",
        commit = "a5f5fd30824f115742827676a7ffff7794a86762", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_build_tools
    )