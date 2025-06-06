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
    # TODO: Return typedb
    git_repository(
        name = "typedb_dependencies",
        remote = "https://github.com/farost/typedb-dependencies",
        commit = "b198f8d11a0b437ee9f9cf1e4eead8bedcdb7312",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
    )

def typeql():
    git_repository(
        name = "typeql",
        remote = "https://github.com/typedb/typeql",
        tag = "3.2.0",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_driver
    )

def typedb_protocol():
    # TODO: Return typedb
    git_repository(
        name = "typedb_protocol",
        remote = "https://github.com/farost/typedb-protocol",
        commit = "3779844e23f2f6e022445fb8caa6b2f7cd6d99eb",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_protocol
    )

def typedb_behaviour():
    # TODO: Return typedb
    git_repository(
        name = "typedb_behaviour",
        remote = "https://github.com/farost/typedb-behaviour",
        commit = "f7c80395b00a1177194d2648e830f41afb059667",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
    )
