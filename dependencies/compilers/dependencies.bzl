#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
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

def antlr_dependencies():

    git_repository(
        name = "rules_antlr",
        remote = "https://github.com/graknlabs/rules_antlr",
        commit = "a802065dc12736d1360946c4bc689c8c8fe0c25f"
    )

def grpc_dependencies():
    git_repository(
        name = "com_github_grpc_grpc",
        remote = "https://github.com/graknlabs/grpc",
        commit = "4a1528f6f20a8aa68bdbdc9a66286ec2394fc170"
    )

    git_repository(
        name = "stackb_rules_proto",
        remote = "https://github.com/stackb/rules_proto",
        commit = "137014a36f389cfcb4987a567b7bd23a7a259cf9",
    )
