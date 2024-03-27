# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")
package(default_visibility = ["//visibility:public"])

native_java_libraries(
    name = "common",
    srcs = glob(["*/*.java", "*/*/*.java"], exclude=["test/*", "*Test.java", "*/*Test.java"]),
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java/query:query",

        # External Maven Dependencies
        "@maven//:com_eclipsesource_minimal_json_minimal_json",
        "@maven//:com_google_ortools_ortools_java",
        "@maven//:com_github_ben_manes_caffeine_caffeine",
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:io_netty_netty_all",
        "@maven//:io_netty_netty_handler",
        "@maven//:io_netty_netty_buffer",
        "@maven//:io_netty_netty_codec_http",
        "@maven//:io_netty_netty_transport",
        "@maven//:io_sentry_sentry",
        "@maven//:org_slf4j_slf4j_api",
    ],
    linux_arm64_deps = ["@maven//:com_google_ortools_ortools_linux_aarch64"],
    linux_x86_64_deps = ["@maven//:com_google_ortools_ortools_linux_x86_64"],
    mac_arm64_deps = ["@maven//:com_google_ortools_ortools_darwin_aarch64"],
    mac_x86_64_deps = ["@maven//:com_google_ortools_ortools_darwin_x86_64"],
    windows_x86_64_deps = ["@maven//:com_google_ortools_ortools_win32_x86_64"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-common:{pom_version}"],
)

host_compatible_java_test(
    name = "test-permutation-iterator",
    srcs = [
        "iterator/PermutationIteratorTest.java",
    ],
    test_class = "com.vaticle.typedb.core.common.iterator.PermutationIteratorTest",
    deps = [
        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
    ],
    native_libraries_deps = [
        "//common:common",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "mpl-header",
)
