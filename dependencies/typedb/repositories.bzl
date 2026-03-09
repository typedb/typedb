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
        commit = "efacbdb7cc0731714586951ff83a19efa27b6e3e",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_dependencies
    )

def typeql():
    # TODO: Return ref after merge to master
    git_repository(
        name = "typeql",
        remote = "https://github.com/typedb/typeql",
        commit = "bff6f17c22650f7f540100180b869c88b2f82270",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_driver
    )

def typedb_protocol():
    # TODO: Return ref after merge to master
    git_repository(
        name = "typedb_protocol",
        remote = "https://github.com/typedb/typedb-protocol",
        commit = "ed427b5eee55e22eac16fec74950a3c394d5372c",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_protocol
    )

def typedb_behaviour():
    # TODO: Update ref after merge to master
    git_repository(
        name = "typedb_behaviour",
        remote = "https://github.com/typedb/typedb-behaviour",
        commit = "59275d9a3f4c37517b601022ba349bf4beabc19c",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @typedb_behaviour
    )
