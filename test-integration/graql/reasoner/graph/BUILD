#
# Copyright (C) 2020 Grakn Labs
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

load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "diagonal-graph",
    srcs = ["DiagonalGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:diagonal-test"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/util:graql-test-util",
    ],
)

java_library(
    name = "dual-linear-transitivity-graph",
    srcs = ["DualLinearTransitivityMatrixGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:dual-linear-transitivity"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/util:graql-test-util",
    ],
)

java_library(
    name = "generic-schema-graph",
    srcs = ["GenericSchemaGraph.java"],
    data = ["//test-integration/graql/reasoner/resources:generic-schema"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/graql/reasoner:pattern",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_library(
    name = "geo-graph",
    srcs = ["GeoGraph.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//kb/concept/api",
        "//kb/server",
        "@graknlabs_graql//java:graql",
    ],
)

java_library(
    name = "linear-transitivity-matrix-graph",
    srcs = ["LinearTransitivityMatrixGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:linear-transivity"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/util:graql-test-util",
    ],
)

java_library(
    name = "nguyen-graph",
    srcs = ["NguyenGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources/recursion:nguyen"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/util:graql-test-util",
    ],
)

java_library(
    name = "path-matrix-graph",
    srcs = ["PathMatrixGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:path-test"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/util:graql-test-util",
    ],
)

java_library(
    name = "path-tree-graph",
    srcs = ["PathTreeGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:path-test"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/util:graql-test-util",
    ],
)

java_library(
    name = "path-tree-symmetric-graph",
    srcs = [
        "PathTreeGraph.java",
        "PathTreeSymmetricGraph.java",
    ],
    resources = ["//test-integration/graql/reasoner/resources:path-test-symmetric"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/util:graql-test-util",
    ],
)

java_library(
    name = "reachability-graph",
    srcs = [
        "ReachabilityGraph.java",
    ],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/util:graql-test-util",
    ],
)

java_library(
    name = "tail-recursion-graph",
    srcs = ["TailRecursionGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources/recursion:tail-recursion"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/util:graql-test-util",
    ],
)

java_library(
    name = "transitivity-chain-graph",
    srcs = ["TransitivityChainGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:quadratic-transitivity"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/util:graql-test-util",
    ],
)

java_library(
    name = "transitivity-matrix-graph",
    srcs = ["TransitivityMatrixGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:quadratic-transitivity"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":diagonal-graph",
        ":dual-linear-transitivity-graph",
        ":generic-schema-graph",
        ":geo-graph",
        ":linear-transitivity-matrix-graph",
        ":nguyen-graph",
        ":path-matrix-graph",
        ":path-tree-graph",
        ":path-tree-symmetric-graph",
        ":reachability-graph",
        ":tail-recursion-graph",
        ":transitivity-chain-graph",
        ":transitivity-matrix-graph",
    ],
)
