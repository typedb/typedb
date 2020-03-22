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
        commit = "ac0b4fb387ce791189938467c0f40fd6fbd15499",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_build_tools
    )

def graknlabs_common():
    git_repository(
        name = "graknlabs_common",
        remote = "https://github.com/graknlabs/common",
        commit = "dfa7ce734d7a50741b63aa909835678a469f73ed",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_common
    )

def graknlabs_graql():
    git_repository(
        name = "graknlabs_graql",
        remote = "https://github.com/graknlabs/graql",
        commit = "4524e58e3d33b7ae38afe022c8126617f4aa19aa",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_graql
    )

def graknlabs_protocol():
    git_repository(
        name = "graknlabs_protocol",
        remote = "https://github.com/graknlabs/protocol",
        commit = "1c0090c847e29fab57e18d9ecd90398515ded67a",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_protocol
    )

def graknlabs_client_java():
    git_repository(
        name = "graknlabs_client_java",
        remote = "https://github.com/graknlabs/client-java",
        commit = "c37f67a6a95e93d9647403b08aa3421cc047e1a7",
    )

def graknlabs_console():
    git_repository(
        name = "graknlabs_console",
        remote = "https://github.com/graknlabs/console",
        tag = "1.0.3",
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
        commit = "c5edff018df75d96300c70bcc667f38570b389b0",
    )

def graknlabs_verification():
    git_repository(
        name = "graknlabs_verification",
        remote = "git@github.com:graknlabs/verification.git",
        commit = "22e20f955884c94d0667001e13e346cb80ebd92d",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_verification
    )
