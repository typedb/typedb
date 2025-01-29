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
        commit = "3348848e9455c1e4984a91c98436f77a8807717a",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
    )

def typeql():
    git_repository(
        name = "typeql",
        remote = "https://github.com/typedb/typeql",
        tag = "3.0.4",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typeql
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
        commit = "58500194ea61782a141d90265ba33f5f6d4b34d6",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
    )
