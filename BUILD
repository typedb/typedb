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

load("@graknlabs_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_zip")
load("@graknlabs_dependencies//tool/release:rules.bzl", "release_validate_deps")
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@graknlabs_dependencies//builder/java:rules.bzl", "native_java_libraries")

exports_files(
    ["VERSION", "deployment.bzl", "RELEASE_TEMPLATE.md", "LICENSE", "README.md"],
)

native_java_libraries(
    name = "grakn",
    srcs = glob(["*.java"]),
    deps = [
        # Internal dependencies
        "//common:common",
    ],
    native_libraries_deps = [
        "//query:query",
        "//concept:concept",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-core:{pom_version}"],
    visibility = ["//visibility:public"],
)

assemble_files = {
    "//server/conf:logback": "server/conf/logback.xml",
    "//server/conf:logback-debug": "server/conf/logback-debug.xml",
    "//server/conf:grakn-properties": "server/conf/grakn.properties",
    "//server/resources:logo": "server/resources/grakn-core-ascii.txt",
    "//:LICENSE": "LICENSE",
}

assemble_deps_common = [
    "//server:server-deps-dev",
#    "//server:server-deps-prod",
    "@graknlabs_common//binary:assemble-bash-targz",
    "@graknlabs_console_artifact//file"
]

assemble_targz(
    name = "assemble-linux-targz",
    targets = assemble_deps_common + ["//server:server-deps-linux"],
    additional_files = assemble_files,
    output_filename = "grakn-core-all-linux",
)

assemble_zip(
    name = "assemble-mac-zip",
    targets = assemble_deps_common + ["//server:server-deps-mac"],
    additional_files = assemble_files,
    output_filename = "grakn-core-all-mac",
)

assemble_zip(
    name = "assemble-windows-zip",
    targets = assemble_deps_common + ["//server:server-deps-windows"],
    additional_files = assemble_files,
    output_filename = "grakn-core-all-windows",
)

release_validate_deps(
    name = "release-validate-deps",
    refs = "@graknlabs_grakn_core_workspace_refs//:refs.json",
    tagged_deps = [
        "@graknlabs_common",
        "@graknlabs_graql",
        "@graknlabs_protocol",
    ],
    tags = ["manual"]  # in order for bazel test //... to not fail
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", ".grabl/*", "bin/*"]),
    exclude = glob(["docs/*"]),
    license_type = "agpl",
)

# CI targets that are not declared in any BUILD file, but are called externally
filegroup(
    name = "ci",
    data = [
        "@graknlabs_dependencies//image/rbe:ubuntu-1604",
        "@graknlabs_dependencies//library/maven:update",
        "@graknlabs_dependencies//tool/checkstyle:test-coverage",
        "@graknlabs_dependencies//tool/sonarcloud:code-analysis",
        "@graknlabs_dependencies//tool/unuseddeps:unused-deps",
    ],
)
