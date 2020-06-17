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
        commit = "715e4802ea375abda44e24b2bb092ee14de72e42",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_build_tools
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
        commit = "4e37a87cb562e47f04c270c6147eac2567d4a223",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_graql
    )

def graknlabs_protocol():
    git_repository(
        name = "graknlabs_protocol",
        remote = "https://github.com/graknlabs/protocol",
        commit = "8ba9a34eb208195fed6c98d4cfc11c8036a83c92",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_protocol
    )

def graknlabs_client_java():
    git_repository(
        name = "graknlabs_client_java",
        remote = "https://github.com/adammitchelldev/client-java",
        commit = "8f0892a3e5ede90496248485ce4534f36aec599c",
    )

def graknlabs_console():
    git_repository(
        name = "graknlabs_console",
        remote = "https://github.com/adammitchelldev/console",
        commit = "370bfc172a276e6990235c316b3089bf50adbaf8", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_console
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
        remote = "https://github.com/graknlabs/verification",
        commit = "902c3846e5e98f02f7b22ff5f2371da6053b88c4",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_verification
    )

def graknlabs_grabl_tracing():
    git_repository(
        name = "graknlabs_grabl_tracing",
        remote = "https://github.com/graknlabs/grabl-tracing",
        commit = "c533775da0af19c5913e725486d68cc849b0dbdb", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_grabl_tracing
    )
