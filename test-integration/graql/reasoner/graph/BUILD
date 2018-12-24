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

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_library(
    name = "diagonal-graph",
    srcs = ["DiagonalGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:diagonal-test"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "diagonal-graph-checkstyle",
 target = ":diagonal-graph",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_library(
    name = "dual-linear-transitivity-graph",
    srcs = ["DualLinearTransitivityMatrixGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:dual-linear-transitivity"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "dual-linear-transitivity-graph-checkstyle",
 target = ":dual-linear-transitivity-graph",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_library(
    name = "generic-schema-graph",
    srcs = ["GenericSchemaGraph.java"],
    data = ["//test-integration/graql/reasoner/resources:generic-schema"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/graql/reasoner/pattern:query-pattern",
        "//test-integration/graql/reasoner/pattern:relation-pattern",
        "//test-integration/graql/reasoner/pattern:resource-pattern",
        "//test-integration/graql/reasoner/pattern:type-pattern",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "generic-schema-graph-checkstyle",
 target = ":generic-schema-graph",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_library(
    name = "geo-graph",
    srcs = ["GeoGraph.java"],
    visibility = ["//test-integration:__subpackages__"],
    deps = ["//server"],
)

checkstyle_test(
 name = "geo-graph-checkstyle",
 target = ":geo-graph",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_library(
    name = "linear-transitivity-matrix-graph",
    srcs = ["LinearTransitivityMatrixGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:linear-transivity"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "linear-transitivity-matrix-graph-checkstyle",
 target = ":linear-transitivity-matrix-graph",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_library(
    name = "nguyen-graph",
    srcs = ["NguyenGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources/recursion:nguyen"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "nguyen-graph-checkstyle",
 target = ":nguyen-graph",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_library(
    name = "path-matrix-graph",
    srcs = ["PathMatrixGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:path-test"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "path-matrix-graph-checkstyle",
 target = ":path-matrix-graph",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],
)

java_library(
    name = "path-tree-graph",
    srcs = ["PathTreeGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:path-test"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "path-tree-graph-checkstyle",
 target = ":path-tree-graph",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],
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
        "//server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "path-tree-symmetric-graph-checkstyle",
 target = ":path-tree-symmetric-graph",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_library(
    name = "tail-recursion-graph",
    srcs = ["TailRecursionGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources/recursion:tail-recursion"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "tail-recursion-graph-checkstyle",
 target = ":tail-recursion-graph",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_library(
    name = "transitivity-chain-graph",
    srcs = ["TransitivityChainGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:quadratic-transitivity"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "transitivity-chain-graph-checkstyle",
 target = ":transitivity-chain-graph",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_library(
    name = "transitivity-matrix-graph",
    srcs = ["TransitivityMatrixGraph.java"],
    resources = ["//test-integration/graql/reasoner/resources:quadratic-transitivity"],
    visibility = ["//test-integration:__subpackages__"],
    deps = [
        "//server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "transitivity-matrix-graph-checkstyle",
 target = ":transitivity-matrix-graph",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)
