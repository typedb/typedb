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
        commit = "f0e5ac771ead5ea8d6e768cbd9b6746b64ed00b1",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_dependencies
    )

def vaticle_typeql():
    git_repository(
        name = "vaticle_typeql",
        remote = "https://github.com/typedb/typeql",
        commit = "9441e3d1472316b3efcd195aab79b0eee6a96572",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typeql
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
        tag = "3.0.0-alpha-7",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_protocol
    )

#def vaticle_typedb_behaviour():
#    git_repository(
#        name = "vaticle_typedb_behaviour",
#        remote = "https://github.com/typedb/typedb-behaviour",
#        commit = "bcc49a3ffd8305f844922a2b36e6f705d9a437de",  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_behaviour
#    )

def vaticle_typedb_behaviour():
    native.local_repository(
        name = "vaticle_typedb_behaviour",
        path = "../typedb-behaviour",
    )
