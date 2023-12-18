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

load("@vaticle_bazel_distribution//github:rules.bzl", "deploy_github")
load("@vaticle_bazel_distribution//maven:rules.bzl", "assemble_maven", "deploy_maven")
load("@vaticle_dependencies//distribution/maven:version.bzl", "version")
load("@vaticle_dependencies//library/maven:artifacts.bzl", "artifacts")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")

load("//:deployment.bzl", deployment_github = "deployment", deployment_docker = "deployment")
load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz", "java_deps")
load("@vaticle_bazel_distribution//artifact:rules.bzl", "deploy_artifact")

java_library(
    name = "typedb-runner",
    srcs = glob(["*.java"]),
    deps = [
        "@vaticle_typedb_common//:common",
        "@maven//:info_picocli_picocli",
        "@maven//:org_zeroturnaround_zt_exec",
        "@maven//:org_slf4j_slf4j_api"
    ],
    visibility = ["//visibility:public"],
    tags = [
        "maven_coordinates=com.vaticle.typedb:typedb-runner:{pom_version}",
    ],
)

assemble_maven(
    name = "assemble-maven",
    target = ":typedb-runner",
    workspace_refs = "@vaticle_typedb_workspace_refs//:refs.json",
    version_overrides = version(artifacts_org = artifacts, artifacts_repo={}),
    project_name = "TypeDB Runners",
    project_description = "TypeDB Runner classes and tools",
)

deploy_maven(
    name = "deploy-maven",
    target = ":assemble-maven",
    snapshot = deployment['maven.snapshot'],
    release = deployment['maven.release']
)

java_deps(
    name = "runner-deps",
    target = ":typedb-runner",
    visibility = ["//:__pkg__"],
    maven_name = True,
    ignore_missing_maven_name = True,
)

assemble_targz(
    name = "assemble-artifact",
    targets = [":runner-deps"],
    output_filename = "typedb-runner",
)

deploy_artifact(
    name = "deploy-artifact",
    target = ":assemble-artifact",
    artifact_group = "vaticle_typedb",
    artifact_name = "typedb-runner-{version}.tar.gz",
    release = deployment['artifact.release'],
    snapshot = deployment['artifact.snapshot'],
)

filegroup(
    name = "logback",
    srcs = ["logback.xml"],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*"]),
    license_type = "agpl-header",
)
