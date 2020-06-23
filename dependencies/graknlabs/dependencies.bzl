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
        commit = "dd58b33370129658aaaf0b2ea25d583fa6b7f06f",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_dependencies
    )

def graknlabs_common():
    git_repository(
        name = "graknlabs_common",
        remote = "https://github.com/lolski/common",
        commit = "60c5f9b2929452ba3011e6bc2f8e67da645eeee1",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_common
    )

def graknlabs_graql():
    git_repository(
        name = "graknlabs_graql",
        remote = "https://github.com/lolski/graql",
        commit = "0a46b5da5fb09756299057010d65f6d563bb5210",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_graql
    )

def graknlabs_protocol():
    git_repository(
        name = "graknlabs_protocol",
        remote = "https://github.com/lolski/protocol",
        commit = "344b232cb4bc23140124abd011370663bcd26776",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_protocol
    )

def graknlabs_client_java():
    # native.local_repository(
    #     name = "graknlabs_client_java",
    #     path = "/Users/lolski/grakn.ai/graknlabs/client-java"    
    # )

    git_repository(
        name = "graknlabs_client_java",
        remote = "https://github.com/lolski/client-java",
        commit = "86c02cbee353b3f22bf5c8b4a357b25250c13c56",
    )

def graknlabs_console():
    # native.local_repository(
    #     name = "graknlabs_console",
    #     path = "/Users/lolski/grakn.ai/graknlabs/console"
    # )
    git_repository(
        name = "graknlabs_console",
        remote = "https://github.com/lolski/console",
        commit = "00f8f304c37201b7ad8ecee7229d81a534a8a7f6", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_console
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
        commit = "db5195de97fbfe151ee7e07aa0f2b7503524c47a",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_verification
    )

def graknlabs_grabl_tracing():
    git_repository(
        name = "graknlabs_grabl_tracing",
        remote = "https://github.com/lolski/grabl-tracing",
        commit = "edd77982151477fbd2ffece6fa43c989ffc23e7a", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_grabl_tracing
    )
