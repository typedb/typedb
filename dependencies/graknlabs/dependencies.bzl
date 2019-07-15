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

def graknlabs_build_tools():
    git_repository(
        name = "graknlabs_build_tools",
        remote = "https://github.com/graknlabs/build-tools",
        commit = "c757f997a7a6530f21ef15898ff025af8091fb3b", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_build_tools
    )

def graknlabs_graql():
     git_repository(
         name = "graknlabs_graql",
         remote = "https://github.com/graknlabs/graql",
         commit = "530e74e23776f235ea37ff5dd15c82d1b6e48fd2", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_graql
     )

def graknlabs_protocol():
    git_repository(
        name = "graknlabs_protocol",
        remote = "https://github.com/graknlabs/protocol",
        commit = "f4770f3923ac4b021329a374e90896b4886f30e1", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_protocol
    )

def graknlabs_client_java():
     git_repository(
         name = "graknlabs_client_java",
         remote = "https://github.com/graknlabs/client-java",
         commit = "d2c3d0c6a764e939d64005d45075c5795055084e",
     )

def graknlabs_benchmark():
    git_repository(
        name = "graknlabs_benchmark",
        remote = "https://github.com/graknlabs/benchmark.git",
        commit = "186eeabf8122c209cc7d0ab290c9fe82b2185cc8" # keep in sync with protocol changes
    )
