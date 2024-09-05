# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def vaticle_bazel_distribution():
    git_repository(
        name = "vaticle_bazel_distribution",
        remote = "https://github.com/typedb/bazel-distribution",
        commit = "4a01d09ef542a423ced909db9a61291dc0a6acc5",
    )

def vaticle_dependencies():
    git_repository(
        name = "vaticle_dependencies",
        remote = "https://github.com/typedb/dependencies",
        commit = "2831e94d01dd670ae00483d625afe8883ec0a556",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_dependencies
    )

def vaticle_typeql():
    git_repository(
        name = "vaticle_typeql",
        remote = "https://github.com/typedb/typeql",
        commit = "c1aac1f4a29ab76163973a2f1fac1b4e0954ffa7",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typeql
    )

def vaticle_typedb_common():
    git_repository(
        name = "vaticle_typedb_common",
        remote = "https://github.com/typedb/typedb-common",
        tag = "2.25.3",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_common
    )

def vaticle_typedb_protocol():
    git_repository(
        name = "vaticle_typedb_protocol",
        remote = "https://github.com/typedb/typedb-protocol",
        commit = "14a9392a9a7d411043e70a81b75da949bb880f4e",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_protocol
    )

def vaticle_typedb_behaviour():
    git_repository(
        name = "vaticle_typedb_behaviour",
        remote = "https://github.com/typedb/typedb-behaviour",
        commit = "dede3aa1a8d29d9530f447f86d3fa3ee5d5f3a3c", # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_behaviour
    )
