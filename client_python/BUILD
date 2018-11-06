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


load("@org_pubref_rules_proto//python:compile.bzl", "python_grpc_compile")
load("@io_bazel_rules_python//python:python.bzl", "py_library", "py_test")
load("@pypi_dependencies//:requirements.bzl", "requirement")


python_grpc_compile(
    name = "client_python_proto",
    deps = [
        "//protocol/session:session-proto",
        "//protocol/session:answer-proto",
        "//protocol/session:concept-proto",
        "//protocol/keyspace:keyspace-proto",
    ]
)

py_library(
    name = "client_python",
    srcs = [":client_python_proto"] + glob([
        "grakn/__init__.py",
        "grakn/exception/*.py",
        "grakn/service/**/*.py"
    ]),
    deps = [
        requirement("protobuf"),
        requirement("grpcio"),
        requirement("six"),
        requirement("enum34")
    ]
)

py_test(
    name = "test_concept",
    srcs = [
        "tests/integration/base.py",
        "tests/integration/test_concept.py"
    ],
    deps = [
        ":client_python",
        requirement("forbiddenfruit")
    ],
    imports = [".", "./client_python_proto"]
)

py_test(
    name = "test_grakn",
    srcs = [
        "tests/integration/base.py",
        "tests/integration/test_grakn.py"
    ],
    deps = [
        ":client_python",
        requirement("forbiddenfruit")
    ],
    imports = [".", "./client_python_proto"]
)

py_test(
    name = "test_keyspace",
    srcs = [
        "tests/integration/base.py",
        "tests/integration/test_keyspace.py"
    ],
    deps = [
        ":client_python",
        requirement("forbiddenfruit")
    ],
    imports = [".", "./client_python_proto"]
)

test_suite(
    name = "test_integration",
    tests = [
        ":test_concept",
        ":test_grakn",
        ":test_keyspace"
    ]
)
