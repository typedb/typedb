# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
package(default_visibility = ["//visibility:public"])


native_java_libraries(
    name = "concurrent",
    srcs = glob(["*/*.java", "*/*/*.java"], exclude=["test/*", "*Test.java", "*/*Test.java"]),
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",

        # External Maven Dependencies
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:io_netty_netty_all",
        "@maven//:io_netty_netty_transport",
        "@maven//:org_slf4j_slf4j_api",
    ],
    native_libraries_deps = [
        # Internal Repository Dependencies
        "//common:common",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-concurrent:{pom_version}"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "mpl-header",
)
