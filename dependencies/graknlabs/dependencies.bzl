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
        commit = "abbee2441ccb14c5e9ff12eb7668ecc89d1c6b1d",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_build_tools
    )

def graknlabs_common():
    git_repository(
        name = "graknlabs_common",
        remote = "https://github.com/graknlabs/common",
        tag = "0.2.2",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_common
    )

def graknlabs_graql():
    git_repository(
        name = "graknlabs_graql",
        remote = "https://github.com/graknlabs/graql",
        commit = "64bb847b36f2c0a78e62d3f13483af6f26a36f22",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_graql
    )

def graknlabs_protocol():
    git_repository(
        name = "graknlabs_protocol",
        remote = "https://github.com/flyingsilverfin/protocol",
        commit = "87cdf74ed08d9ba541d31494ca5bc8ccccf12ef2",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_protocol
    )

def graknlabs_client_java():
    git_repository(
        name = "graknlabs_client_java",
        remote = "https://github.com/flyingsilverfin/client-java",
        commit = "03aabadd611bdf746f660d9a826f479a9f6b814f",
    )

def graknlabs_console():
    git_repository(
        name = "graknlabs_console",
        remote = "https://github.com/graknlabs/console",
        tag = "1.0.5", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_console
    )

def graknlabs_simulation():
    git_repository(
        name = "graknlabs_simulation",
        remote = "https://github.com/graknlabs/simulation",
        commit = "33ead5a0258be5bd76318d62ebdbacbb1edf6bed", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_simulation
    )

def graknlabs_verification():
    git_repository(
        name = "graknlabs_verification",
        remote = "https://github.com/flyingsilverfin/verification.git",
        commit = "3f0162d1fdaf1ec9c3c2bf2eaa11c5b1086bd379",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_verification
    )

def graknlabs_grabl_tracing():
    git_repository(
        name = "graknlabs_grabl_tracing",
        remote = "https://github.com/graknlabs/grabl-tracing",
        commit = "42f507d6b973cbc87d18a27ee83121c791295184", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_grabl_tracing
    )
