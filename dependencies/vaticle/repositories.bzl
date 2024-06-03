# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def vaticle_bazel_distribution():
    git_repository(
        name = "vaticle_bazel_distribution",
        remote = "https://github.com/vaticle/bazel-distribution",
        commit = "0040e492db47eb83681a62ddf8491a7218633164", # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_bazel_distribution
    )

def vaticle_dependencies():
    git_repository(
        name = "vaticle_dependencies",
        remote = "https://github.com/vaticle/dependencies",
        commit = "8ed4fd2b3bfcfbeb572322364e4b3fb8b77064e0", # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_dependencies
    )

def vaticle_typeql():
    git_repository(
        name = "vaticle_typeql",
        remote = "https://github.com/vaticle/typeql",
        tag = "2.28.1",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typeql
    )

def vaticle_typedb_protocol():
    git_repository(
        name = "vaticle_typedb_protocol",
        remote = "https://github.com/vaticle/typedb-protocol",
        tag = "2.28.0",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_protocol
    )

def vaticle_typedb_behaviour():
    git_repository(
        name = "vaticle_typedb_behaviour",
        remote = "https://github.com/vaticle/typedb-behaviour",
        commit = "3cd409cf6699d3616c5b5c80849f9210833135a2",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_behaviour
    )
