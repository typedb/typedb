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

def graknlabs_dependencies():
    # native.local_repository(
    #     name = "graknlabs_dependencies",
    #     path = "/Users/lolski/grakn.ai/graknlabs/dependencies"
    # )
    git_repository(
        name = "graknlabs_dependencies",
        remote = "https://github.com/lolski/dependencies",
        commit = "52f93cdd1e11f9bab1ba4548415b575fb5d31b52",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_dependencies
    )

def graknlabs_common():
    git_repository(
        name = "graknlabs_common",
        remote = "https://github.com/lolski/common",
        commit = "30931af4a78bd841eb7c5b373723682dc9d344f2",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_common
    )

def graknlabs_graql():
    git_repository(
        name = "graknlabs_graql",
        remote = "https://github.com/lolski/graql",
        commit = "f82aba9fabfd2393a566ac98c21c6d9ea84fe223",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_graql
    )

def graknlabs_protocol():
    git_repository(
        name = "graknlabs_protocol",
        remote = "https://github.com/lolski/protocol",
        commit = "886cb55cb870ad9cee30e59334212515d6554ab0",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_protocol
    )

def graknlabs_client_java():
    # native.local_repository(
    #     name = "graknlabs_client_java",
    #     path = "/Users/lolski/grakn.ai/graknlabs/client-java"    
    # )

    git_repository(
        name = "graknlabs_client_java",
        remote = "https://github.com/lolski/client-java",
        commit = "47898ecefaa186e55ee8824dbc8401057006ba20",
    )

def graknlabs_console():
    # native.local_repository(
    #     name = "graknlabs_console",
    #     path = "/Users/lolski/grakn.ai/graknlabs/console"
    # )
    git_repository(
        name = "graknlabs_console",
        remote = "https://github.com/lolski/console",
        commit = "90581d983ea012d116eff01b028dfd07420e8d80", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_console
    )

def graknlabs_simulation():
    # native.local_repository(
    #     name = "graknlabs_simulation",
    #     path = "/Users/lolski/grakn.ai/graknlabs/simulation"
    # )
    git_repository(
        name = "graknlabs_simulation",
        remote = "https://github.com/graknlabs/simulation",
        commit = "33ead5a0258be5bd76318d62ebdbacbb1edf6bed", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_simulation
    )

def graknlabs_verification():
    # native.local_repository(
    #     name = "graknlabs_verification",
    #     path = "/Users/lolski/grakn.ai/graknlabs/verification"
    # )
    git_repository(
        name = "graknlabs_verification",
        remote = "https://github.com/lolski/verification",
        commit = "f414566c84afcf8704bd88cd882890125f1390f4",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_verification
    )

def graknlabs_grabl_tracing():
    git_repository(
        name = "graknlabs_grabl_tracing",
        remote = "https://github.com/lolski/grabl-tracing",
        commit = "6c758ae6c58cb1853059f6f0e85552d32865b7c8", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_grabl_tracing
    )
