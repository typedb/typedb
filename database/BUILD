# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//library/ortools:deps.bzl", "google_or_tools")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")

package(
    default_visibility = [
        "//server:__pkg__",
        "//test:__subpackages__",
    ]
)

native_java_libraries(
    name = "database",
    srcs = glob(["*.java"]),
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//common:common",
        "//concept:concept",
        "//concurrent:concurrent",
        "//graph:graph",
        "//encoding:encoding",
        "//logic:logic",
        "//query:query",
        "//reasoner:reasoner",
        "//traversal:traversal",
    ],
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",

        # External dependencies from Maven
        "@maven//:com_google_ortools_ortools_java",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:io_github_speedb_io_speedbjni",
        "@maven//:io_sentry_sentry",
    ],
    linux_arm64_deps = ["@maven//:com_google_ortools_ortools_linux_aarch64"],
    linux_x86_64_deps = ["@maven//:com_google_ortools_ortools_linux_x86_64"],
    mac_arm64_deps = ["@maven//:com_google_ortools_ortools_darwin_aarch64"],
    mac_x86_64_deps = ["@maven//:com_google_ortools_ortools_darwin_x86_64"],
    windows_x86_64_deps = ["@maven//:com_google_ortools_ortools_win32_x86_64"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-database:{pom_version}"],
    visibility = [ "//visibility:public" ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
