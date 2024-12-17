# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def typedb_bazel_distribution():
    git_repository(
        name = "typedb_bazel_distribution",
        remote = "https://github.com/typedb/bazel-distribution",
        commit = "94c4f7b1dda39bf187f73c6ea035971c4c91528b",
    )

def typedb_dependencies():
    git_repository(
        name = "typedb_dependencies",
        remote = "https://github.com/typedb/typedb-dependencies",
        commit = "bc2381be8f38a6e55d15f61739dcf04d32e67769", # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
    )

def typeql():
    git_repository(
        name = "typeql",
        remote = "https://github.com/krishnangovindraj/typeql",
        commit = "0db5e4d9cf83173002ce999cf19779847cd52566",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typeql
    )


def typedb_common():
    git_repository(
        name = "typedb_common",
        remote = "https://github.com/typedb/typedb-common",
        tag = "2.25.3",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_common
    )

def typedb_protocol():
    git_repository(
        name = "typedb_protocol",
        remote = "https://github.com/krishnangovindraj/typedb-protocol",
        commit = "62c478634f7458761f4fc38cb6113d8e05fea8a2",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_protocol
    )

def typedb_behaviour():
    git_repository(
        name = "typedb_behaviour",
        remote = "https://github.com/krishnangovindraj/typedb-behaviour",
        commit = "80e07b383c13686ae67be233f6304fd3fbdac138",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
    )
