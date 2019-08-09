#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2019 Grakn Labs Ltd
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

package(default_visibility = ["//visibility:__subpackages__"])
load("@graknlabs_build_tools//distribution/maven:rules.bzl", "assemble_maven", "deploy_maven")
load("@graknlabs_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@graknlabs_bazel_distribution//common:rules.bzl", "assemble_targz", "java_deps", "assemble_zip")
load("@graknlabs_bazel_distribution//rpm:rules.bzl", "assemble_rpm", "deploy_rpm")
load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

genrule(
    name = "version",
    srcs = [
        "templates/Version.java",
        ":VERSION",
    ],
    cmd = "VERSION=`cat $(location :VERSION)`;sed -e \"s/{version}/$$VERSION/g\" $(location templates/Version.java) >> $@",
    outs = ["Version.java"],
    visibility = ["//visibility:public"]
)

java_library(
    name = "console",
    srcs = glob(["**/*.java"], exclude=["templates/**", "**/test/**"]) + [":version"],
    deps = [
        "//concept", # TODO: To be removed with issue #5288
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",


        # External dependencies
        "//dependencies/maven/artifacts/commons-cli",
        "//dependencies/maven/artifacts/commons-lang:commons-lang", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/jline:jline",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
    ],
    visibility = ["//visibility:public"],
    resources = ["LICENSE"],
    resource_strip_prefix = "console",
    tags = ["maven_coordinates=io.grakn.core:grakn-console:{pom_version}"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":console"
    ],
)

exports_files(
    glob(["conf/logback.xml"]),
    visibility = ["//visibility:public"]
)

java_binary(
    name = "console-binary",
    main_class = "grakn.core.console.GraknConsole",
    runtime_deps = [":console"],
    visibility = ["//:__pkg__"],
)

java_deps(
    name = "console-deps",
    target = ":console-binary",
    java_deps_root = "console/services/lib/",
    version_file = "//:VERSION",
    visibility = ["//:__pkg__"]
)

assemble_targz(
    name = "assemble-linux-targz",
    output_filename = "grakn-core-console-linux",
    targets = [":console-deps", "//bin:assemble-bash-targz"],
    additional_files = {
        "//console:conf/logback.xml": "console/conf/logback.xml"
    },
    visibility = ["//visibility:public"]
)

assemble_zip(
    name = "assemble-mac-zip",
    output_filename = "grakn-core-console-mac",
    targets = [":console-deps", "//bin:assemble-bash-targz"],
    additional_files = {
        "//console:conf/logback.xml": "console/conf/logback.xml"
    },
    visibility = ["//visibility:public"]
)

assemble_zip(
    name = "assemble-windows-zip",
    output_filename = "grakn-core-console-windows",
    targets = [":console-deps", "//bin:assemble-bash-targz"],
    additional_files = {
        "//console:conf/logback.xml": "console/conf/logback.xml"
    },
    visibility = ["//visibility:public"]
)

# TODO: To be removed with issue #5269
assemble_maven(
    name = "assemble-maven",
    target = ":console",
    package = "console",
    version_file = "//:VERSION",
    workspace_refs = "@graknlabs_grakn_core_workspace_refs//:refs.json"
)

# TODO: To be removed with issue #5269
deploy_maven(
    name = "deploy-maven",
    target = ":assemble-maven"
)

assemble_apt(
    name = "assemble-linux-apt",
    package_name = "grakn-core-console",
    maintainer = "Grakn Labs <community@grakn.ai>",
    description = "Grakn Core (console)",
    version_file = "//:VERSION",
    depends = [
      "openjdk-8-jre",
      "grakn-core-bin (={version})"
    ],
    files = {
        "//console:conf/logback.xml": "console/conf/logback.xml"
    },
    archives = [":console-deps"],
    installation_dir = "/opt/grakn/core/",
    empty_dirs = [
         "opt/grakn/core/console/services/lib/",
    ],
)

deploy_apt(
    name = "deploy-apt",
    target = ":assemble-linux-apt",
    deployment_properties = "@graknlabs_build_tools//:deployment.properties",
)

assemble_rpm(
    name = "assemble-linux-rpm",
    package_name = "grakn-core-console",
    installation_dir = "/opt/grakn/core/",
    version_file = "//:VERSION",
    spec_file = "//config/rpm:grakn-core-console.spec",
    archives = [":console-deps"],
    files = {
        "//console:conf/logback.xml": "console/conf/logback.xml"
    },
    empty_dirs = [
         "opt/grakn/core/console/services/lib/",
    ],
)

deploy_rpm(
    name = "deploy-rpm",
    target = ":assemble-linux-rpm",
    deployment_properties = "@graknlabs_build_tools//:deployment.properties",
)