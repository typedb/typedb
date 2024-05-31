# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_zip", "java_deps")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

package(default_visibility = ["//visibility:private",])

host_compatible_java_test(
    name = "test-configuration",
    srcs = [
        "parameters/CoreConfigTest.java",
    ],
    native_libraries_deps = [
        "//common:common",
        "//server:server"
    ],
    test_class = "com.vaticle.typedb.core.server.parameters.CoreConfigTest",
    deps = [
        # Internal dependencies

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
    ],
    data = [
        "//server/parameters/config",
        ":configurations"
    ],
)

filegroup(
    name = "configurations",
    srcs = glob(["parameters/config/*.yml"])
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "parameters/config/*"]),
    license_type = "mpl-header",
)
