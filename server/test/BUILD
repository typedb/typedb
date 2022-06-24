#
# Copyright (C) 2022 Vaticle
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
    name = "test-configuration",
    srcs = [
        "CoreConfigTest.java",
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
        "//server:config",
        ":configurations"
    ],
)

filegroup(
    name = "configurations",
    srcs = glob(["config/*.yml"])
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "config/*"]),
    license_type = "agpl",
)
