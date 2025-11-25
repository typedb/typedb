# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def typedb_bazel_distribution():
    git_repository(
        name = "typedb_bazel_distribution",
        remote = "https://github.com/typedb/bazel-distribution",
        commit = "a9a587849ca17b4eea54cc66d6ec0f28cf935da7",
    )

def typedb_dependencies():
    # TODO: Return ref after merge to master, currently points to 'raft-dependencies-addition'
    git_repository(
        name = "typedb_dependencies",
        remote = "https://github.com/typedb/typedb-dependencies",
        commit = "19a70bcad19b9a28814016f183ac3e3a23c1ff0d",  # sync-mark\er: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
    )

def typeql():
    git_repository(
        name = "typeql",
        remote = "https://github.com/typedb/typeql",
        tag = "3.7.0-rc0",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_driver
    )

def typedb_protocol():
    # TODO: Return ref after merge to master
    git_repository(
        name = "typedb_protocol",
        remote = "https://github.com/typedb/typedb-protocol",
        commit = "3bf98f9ee1161af71807d0d95e1c2f0819d12c92",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_protocol
    )

def typedb_behaviour():
    # TODO: Update ref after merge to master
    git_repository(
        name = "typedb_behaviour",
        remote = "https://github.com/typedb/typedb-behaviour",
        commit = "9a09881a377fd515b58f1ee4cbbfd04f4bf67fe8",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
    )
