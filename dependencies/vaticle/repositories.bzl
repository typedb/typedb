#
# Copyright (C) 2021 Vaticle
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def vaticle_dependencies():
    git_repository(
        name = "vaticle_dependencies",
        remote = "https://github.com/lolski/dependencies",
        commit = "2df529c3a23deff7df7c281b5f2858336cd4c759", # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_dependencies
    )
    # native.local_repository(
    #     name = "vaticle_dependencies",
    #     path = "../dependencies"
    # )

def vaticle_typeql_lang_java():
    git_repository(
        name = "vaticle_typeql_lang_java",
        remote = "https://github.com/lolski/typeql-lang-java",
        commit = "c93b777011f367dbf526c7d2c126ef33330e3250", # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typeql_lang_java
    )

def vaticle_typedb_common():
    git_repository(
        name = "vaticle_typedb_common",
        remote = "https://github.com/lolski/typedb-common",
        commit = "eaf68885b73cca04dc73099312ad9d264826a6ec", # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_common
    )

def vaticle_typedb_protocol():
    git_repository(
        name = "vaticle_typedb_protocol",
        remote = "https://github.com/lolski/typedb-protocol",
        commit = "eac201f73c3047f9abb5e75f68a5ab499d0b79ee", # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_protocol
    )

def vaticle_typedb_behaviour():
    git_repository(
        name = "vaticle_typedb_behaviour",
        remote = "https://github.com/lolski/typedb-behaviour",
        commit = "e3f0346c15177425e488b5312e76cc06a3326d3c", # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_typedb_behaviour
    )

def vaticle_factory_tracing():
    git_repository(
        name = "vaticle_factory_tracing",
        remote = "https://github.com/lolski/factory-tracing",
        commit = "4752373623483e41aa5d8c99e093039393ccee6f"  # sync-marker: do not remove this comment, this is used for sync-dependencies by @vaticle_factory_tracing
    )
