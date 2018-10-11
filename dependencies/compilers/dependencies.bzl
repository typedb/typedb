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

def antlr_dependencies():

    native.git_repository(
        name = "rules_antlr",
        remote = "https://github.com/marcohu/rules_antlr",
        tag = "0.1.0" # sync version with //dependencies/maven/artifacts/org/antlr
    )

def grpc_dependencies():

    native.git_repository(
        name = "com_github_grpc_grpc",
        remote = "https://github.com/grpc/grpc",
        tag = "v1.15.1"
    )

    native.git_repository(
        name = "org_pubref_rules_proto",
        remote = "https://github.com/graknlabs/rules_proto",
        commit = "9fd93eb7a81e4d6082d2236ba492e13338080945",
    )

def python_dependencies():
    native.git_repository(
        name = "io_bazel_rules_python",
        remote = "https://github.com/graknlabs/rules_python.git",
        commit = "abd475a72ae6a098cc9f859eb435dddd992bc884",
        sha256 = "fe468b9396ef5c933679e1a5d846f777d0ea4731927df2149e5a01b328afd9b6"
    )
