# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")

package(default_visibility = [
    "//test/benchmark/reasoner/iam:__subpackages__",
])

native_java_libraries(
    name = "common",
    srcs = ["BenchmarkRunner.java",
            "Benchmark.java",
            "QueryParams.java",
    ],
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//common:common",
        "//concurrent:concurrent",
        "//migrator:migrator",
        "//reasoner:reasoner",
    ],
    deps = [
        "//server:version",

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:junit_junit",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
