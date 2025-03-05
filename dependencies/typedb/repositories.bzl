# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def typedb_bazel_distribution():
    git_repository(
        name = "typedb_bazel_distribution",
        remote = "https://github.com/typedb/bazel-distribution",
        commit = "056a8d7ede9b552d23dcfdc2d47b9395510652f4",
    )

def typedb_dependencies():
    # TODO: return typedb
    git_repository(
        name = "typedb_dependencies",
        remote = "https://github.com/farost/dependencies",
        commit = "76245eebd5c2ce4ec73b0e2bf70b812809255077",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
    )
#    git_repository(
#        name = "typedb_dependencies",
#        remote = "https://github.com/typedb/typedb-dependencies",
#        commit = "b22f2877e1a3677a5758cb1d799503ff676cb137",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
#    )
#    native.local_repository(
#        name = "typedb_dependencies",
#        path = "../dependencies",
#    )


def typeql():
    # TODO: return typedb
    git_repository(
        name = "typeql",
        remote = "https://github.com/farost/typeql",
        commit = "5ccc1da728b381286b66cf059df010586eb60b48",
    )
#    git_repository(
#        name = "typeql",
#        remote = "https://github.com/typedb/typeql",
#        commit = "c3651b6d3b82f6dc8a0db499592cb22e60e79b49",
#    )

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
        commit = "ae5dcc5b8ff854c5d72fd91b5a3970b79833aeba",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
    )
