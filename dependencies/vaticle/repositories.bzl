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
        remote = "https://github.com/krishnangovindraj/typeql",
        commit = "061804e6484fbc68eee746b0610a7b4ff126d9a0",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typeql
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
        commit = "34e64e7a48af44b3b6b07a5924430f03409dac27",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_protocol
    )

def vaticle_typedb_behaviour():
    native.local_repository(
        name = "vaticle_typedb_behaviour",
        path = "../typedb-behaviour",
    )
#    git_repository(
#        name = "vaticle_typedb_behaviour",
#        remote = "https://github.com/krishnangovindraj/typedb-behaviour",
#        commit = "1b71798da18c1b004fe5fb6d03746a099004f9c1", # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_behaviour
#    )
