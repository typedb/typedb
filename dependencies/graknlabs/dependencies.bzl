#
# Copyright (C) 2020 Grakn Labs
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
        commit = "fb6e02f964e8f1742febe788ba3dbbcc17551cb0",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_build_tools
    )

def graknlabs_common():
    git_repository(
        name = "graknlabs_common",
        remote = "https://github.com/graknlabs/common",
        commit = "9e590131790cd6a79326a31dbe687d77ea629194",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_common
    )

def graknlabs_graql():
    git_repository(
        name = "graknlabs_graql",
        remote = "https://github.com/graknlabs/graql",
        commit = "4d2a3afd0b8e5c8ed43677610e74780ecaaacba2",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_graql
    )

def graknlabs_protocol():
    git_repository(
        name = "graknlabs_protocol",
        remote = "https://github.com/graknlabs/protocol",
        commit = "7af1e68d30335db509d220dcdc2f050177101227",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_protocol
    )

def graknlabs_client_java():
    git_repository(
        name = "graknlabs_client_java",
#        remote = "https://github.com/graknlabs/client-java",
#        commit = "4df3911d06e37508c21880e74fa5a545a8b65f37",
         remote = "https://github.com/flyingsilverfin/client-java",
         commit = "5c4159b38686a43dc8a2c25fed7f6af2ade15a8e", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_client_java

    )

def graknlabs_console():
    git_repository(
        name = "graknlabs_console",
#        remote = "https://github.com/graknlabs/console",
#        tag = "1.0.3",
        remote = "https://github.com/flyingsilverfin/console",
        commit = "0a0149af8ae27ae332c193cca4ed5b3b098b5b6c"
    )

def graknlabs_benchmark():
    git_repository(
        name = "graknlabs_benchmark",
        remote = "https://github.com/graknlabs/benchmark.git",
        commit = "78869c68a2b6073917f7d6ee085ffd0b0d6a29b0",  # keep in sync with protocol changes
    )

def graknlabs_simulation():
    git_repository(
        name = "graknlabs_simulation",
        remote = "https://github.com/graknlabs/simulation",
        commit = "33ead5a0258be5bd76318d62ebdbacbb1edf6bed", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_protocol
    )

def graknlabs_verification():
    git_repository(
        name = "graknlabs_verification",
        remote = "git@github.com:graknlabs/verification.git",
        commit = "22e20f955884c94d0667001e13e346cb80ebd92d",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_verification
    )

def graknlabs_grabl_tracing():
    git_repository(
        name = "graknlabs_grabl_tracing",
        remote = "https://github.com/graknlabs/grabl-tracing",
        commit = "42f507d6b973cbc87d18a27ee83121c791295184", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_protocol
    )
