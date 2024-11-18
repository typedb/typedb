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
        commit = "70e8b662d2b3f10bba64befcc2c5183949eb9efa", # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
    )

def typeql():
    git_repository(
        name = "typeql",
        remote = "https://github.com/typedb/typeql",
        commit = "0be38cbab19da7d6b2549ad5aa1248fc811bc4e0",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typeql
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
        commit = "8e316bb783a6315c881f3cba54d9afe52fef5edb",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_protocol
    )

def typedb_behaviour():
    # TODO: return typedb
    git_repository(
        name = "typedb_behaviour",
        remote = "https://github.com/farost/typedb-behaviour",
        commit = "5abd003b01a3d5e994901ca8a991602f46d7128a",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
    )
