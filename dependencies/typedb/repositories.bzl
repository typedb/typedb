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
        commit = "52191031bb1231c714a2f2017571d61c81b4a890",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
    )

def typeql():
    git_repository(
        name = "typeql",
        remote = "https://github.com/typedb/typeql",
        commit = "059c0332e33a892e6a04bbf83df5634d3d5df249",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typeql
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
        remote = "https://github.com/typedb/typedb-protocol",
        tag = "3.0.0",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_protocol
    )

def typedb_behaviour():
    git_repository(
        name = "typedb_behaviour",
        remote = "https://github.com/typedb/typedb-behaviour",
        commit = "0dcd9887f3145e28658aa1dc30eebfd65dbd93e6",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
    )
