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
    git_repository(
        name = "graknlabs_dependencies",
        remote = "https://github.com/lolski/dependencies",
        commit = "c1a12d49054e782953b899d5528825e546a5b76c",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_dependencies
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
    git_repository(
        name = "graknlabs_client_java",
        remote = "https://github.com/lolski/client-java",
        commit = "51da498c62e82637810aed1bb8e9571a10c2c617",
    )

def graknlabs_console():
    # native.local_repository(
    #     name = "graknlabs_console",
    #     path = "/Users/lolski/grakn.ai/graknlabs/console"
    # )
    git_repository(
        name = "graknlabs_console",
        remote = "https://github.com/lolski/console",
        commit = "e424cd2d2fc1c43a065696980d84cabf444eb01b", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_console
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
    git_repository(
        name = "graknlabs_verification",
        remote = "https://github.com/alexjpwalker/verification",
        commit = "bef8d336123c32cc807cb1e4da416fcf756dd15e",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_verification
    )

def graknlabs_grabl_tracing():
    git_repository(
        name = "graknlabs_grabl_tracing",
        remote = "https://github.com/lolski/grabl-tracing",
        commit = "0031b39eebea2e2844a37d82800f456547deff3d", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_grabl_tracing
    )
