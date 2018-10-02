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

    native.http_archive(
        name = "rules_antlr",
        sha256 = "acd2a25f31aeeea5f58cdb434ae109d03826ae7cc11fe9efce1740102e3f4531",
        strip_prefix = "rules_antlr-0.1.0",
        urls = ["https://github.com/marcohu/rules_antlr/archive/0.1.0.tar.gz"],
    )


def grpc_dependencies():

    native.git_repository(
        name = "com_github_grpc_grpc",
        remote = "https://github.com/grpc/grpc",
        tag = "v1.15.1"
    )

    native.git_repository(
        name = "org_pubref_rules_proto",
        remote = "https://github.com/pubref/rules_proto",
        commit = "27da2e7af9e4a1c43c584be2f05be8a301a642b6",
    )