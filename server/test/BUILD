#
# Copyright (C) 2021 Vaticle
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

load("@vaticle_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@vaticle_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_zip", "java_deps")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")
load("@stackb_rules_proto//java:java_grpc_compile.bzl", "java_grpc_compile")

package(default_visibility = ["//visibility:private",])

host_compatible_java_test(
    name = "test-config",
    srcs = [
        "ConfigurationTest.java",
    ],
    native_libraries_deps = [
        "//common:common",
        "//server:server"
    ],
    test_class = "com.vaticle.typedb.core.server.test.ConfigurationTest",
    deps = [
        # Internal dependencies

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
    ],
    data = [
        "//server/conf:typedb-defaults",
        ":configuration-minimal-abs-paths",
        ":configuration-invalid-logger-output",
        ":configuration-missing-debugger",
        ":configuration-missing-port",
        ":configuration-wrong-path-type",
        ":configuration-unrecognised-option"
    ],
)

filegroup(
    name = "configuration-minimal-abs-paths",
    srcs = ["config_minimal_abs_path.yml"],
)

filegroup(
    name = "configuration-invalid-logger-output",
    srcs = ["config_invalid_logger_output.yml"],
)

filegroup(
    name = "configuration-missing-debugger",
    srcs = ["config_missing_debugger.yml"],
)

filegroup(
    name = "configuration-missing-port",
    srcs = ["config_missing_port.yml"],
)

filegroup(
    name = "configuration-wrong-path-type",
    srcs = ["config_wrong_path_type.yml"],
)

filegroup(
    name = "configuration-unrecognised-option",
    srcs = ["config_unrecognised_option.yml"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl",
)
