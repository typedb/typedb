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
    git_repository(
        name = "typedb_dependencies",
        remote = "https://github.com/typedb/typedb-dependencies",
        commit = "959bcdbfac995582812b334ba719b190367e4625",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
    )

def typeql():
    git_repository(
        name = "typeql",
        remote = "https://github.com/typedb/typeql",
        commit = "e8725d27a82d4b3a31da72821ae70309242c7f86"
    )

def typedb_protocol():
    git_repository(
        name = "typedb_protocol",
        remote = "https://github.com/typedb/typedb-protocol",
        commit = "9e46e089a005d6ca9f017ffb482337b9d5718695",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_protocol
    )

def typedb_behaviour():
    # TODO: Return typedb
    git_repository(
        name = "typedb_behaviour",
        remote = "https://github.com/farost/typedb-behaviour",
        commit = "6b018844b9c0334e3fc3d2e4096f70f5ac29ba69",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
    )
#    git_repository(
#        name = "typedb_behaviour",
#        remote = "https://github.com/typedb/typedb-behaviour",
#        commit = "a5ca738d691e7e7abec0a69e68f6b06310ac2168",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
#    )
