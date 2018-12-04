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

exports_files([
    "package.json",
    "package-lock.json"
])

load("@stackb_rules_proto//node:node_grpc_compile.bzl", "node_grpc_compile")
load("@build_bazel_rules_nodejs//:defs.bzl", "npm_package", "nodejs_jest_test")


node_grpc_compile(
    name = "client-nodejs-proto",
    deps = [
        "//protocol/session:session-proto",
        "//protocol/session:answer-proto",
        "//protocol/session:concept-proto",
        "//protocol/keyspace:keyspace-proto",
    ]
)

npm_package(
    name = "client-nodejs",
    srcs = glob([
            "src/*.js",
            "src/service/Keyspace/KeyspaceService.js",
            "src/service/Session/*.js",
            "src/service/Session/util/*.js",
            "src/service/Session/concept/*.js",
        ]),
    deps = [
        ":client-nodejs-proto",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf"
    ]
)

nodejs_jest_test(
    name = "keyspace-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/keyspace/Keyspace.test.js"
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf"
    ]
)

nodejs_jest_test(
    name = "concept-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/Concept.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

nodejs_jest_test(
    name = "schemaconcept-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/SchemaConcept.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

nodejs_jest_test(
    name = "rule-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/Rule.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

nodejs_jest_test(
    name = "type-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/Type.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

nodejs_jest_test(
    name = "attribute-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/Attribute.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

nodejs_jest_test(
    name = "attributetype-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/AttributeType.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

nodejs_jest_test(
    name = "role-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/Role.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

nodejs_jest_test(
    name = "grakntx-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/GraknTx.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

nodejs_jest_test(
    name = "relationship-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/Relationship.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

nodejs_jest_test(
    name = "relationshiptype-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/RelationshipType.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

nodejs_jest_test(
    name = "thing-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/Thing.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

nodejs_jest_test(
    name = "entitytype-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/EntityType.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

nodejs_jest_test(
    name = "committx-test",
    srcs = [
        "tests/support/GraknTestEnvironment.js",
        "tests/service/session/transaction/CommitTx.test.js",
    ],
    deps = [
        ":client-nodejs",
        "@nodejs_dependencies//jest",
        "@nodejs_dependencies//fs-extra",
        "@nodejs_dependencies//grpc",
        "@nodejs_dependencies//google-protobuf",
    ]
)

test_suite(
    name = "test-integration",
    tests = [
        ":keyspace-test",
        ":concept-test",
        ":schemaconcept-test",
        ":rule-test",
        ":type-test",
        ":attribute-test",
        ":attributetype-test",
        ":role-test",
        ":grakntx-test",
        ":relationship-test",
        ":relationshiptype-test",
        ":thing-test",
        ":entitytype-test",
        ":committx-test",
    ]
)
