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
        remote = "https://github.com/graknlabs/grpc",
        commit = "da829a5ac902ab99eef14e6aad1d8e0cd173ec64"
    )

    native.git_repository(
        name = "stackb_rules_proto",
        remote = "https://github.com/stackb/rules_proto",
        commit = "4c2226458203a9653ae722245cc27e8b07c383f7",
    )
