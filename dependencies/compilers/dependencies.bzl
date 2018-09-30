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


def protobuf_dependencies():

    native.http_archive(
        name = "bazel_skylib",
        sha256 = "bbccf674aa441c266df9894182d80de104cabd19be98be002f6d478aaa31574d",
        strip_prefix = "bazel-skylib-2169ae1c374aab4a09aa90e65efe1a3aad4e279b",
        urls = ["https://github.com/bazelbuild/bazel-skylib/archive/2169ae1c374aab4a09aa90e65efe1a3aad4e279b.tar.gz"],
    )

    native.bind(name = "six", actual = "@six_archive//:six")
    native.new_http_archive(
       name = "six_archive",
       build_file = "@com_google_protobuf//:six.BUILD",
       sha256 = "105f8d68616f8248e24bf0e9372ef04d3cc10104f1980f54d57b2ce73a5ad56a",
       urls = ["https://pypi.python.org/packages/source/s/six/six-1.10.0.tar.gz#md5=34eed507548117b2ab523ab14b2f8b55"],
    )

    # This com_google_protobuf repository is required for proto_library rule.
    # It provides the protocol compiler binary (i.e., protoc).
    native.http_archive(
        name = "com_google_protobuf",
        sha256 = "0a4c6d0678eb2f063df332cff1a41647ef692c067b5cfb19e51bca778e79d9e0",
        strip_prefix = "protobuf-3.6.1",
        urls = ["https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protobuf-all-3.6.1.zip"],
    )

