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
    # TODO: update to graknlabs/graql before merging the PR
     git_repository(
         name = "graknlabs_graql",
         remote = "https://github.com/lolski/graql",
         commit = "d6f87c59deaae901f0ac7ecbdc395d17ac88a5f2",
     )

def graknlabs_client_java():
    # TODO: update to graknlabs/client-java before merging the PR
     git_repository(
         name = "graknlabs_client_java",
         remote = "https://github.com/lolski/client-java",
         commit = "429aff03dfd5bdb7d7f6357ce5a65c8dfe4d044b",
     )

def graknlabs_build_tools():
    # TODO: update to graknlabs/graql before merging the PR
    git_repository(
        name = "graknlabs_build_tools",
        remote = "https://github.com/lolski/build-tools",
        commit = "bd3c9df990e7fc464cebc9091c9309401d74f8b4", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_build_tools
    )