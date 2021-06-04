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

load("//:deployment.bzl", deployment_github = "deployment", deployment_docker = "deployment")
load("@vaticle_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@vaticle_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@vaticle_bazel_distribution//brew:rules.bzl", "deploy_brew")
load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_zip", "checksum", "assemble_versioned")
load("@vaticle_bazel_distribution//github:rules.bzl", "deploy_github")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//distribution/artifact:rules.bzl", "artifact_repackage")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//tool/release:rules.bzl", "release_validate_deps")
load("@io_bazel_rules_docker//container:bundle.bzl", "container_bundle")
load("@io_bazel_rules_docker//container:image.bzl", "container_image")
load("@io_bazel_rules_docker//contrib:push-all.bzl", "docker_push")

exports_files(
    ["VERSION", "deployment.bzl", "RELEASE_TEMPLATE.md", "LICENSE", "README.md"],
)

native_java_libraries(
    name = "typedb",
    srcs = glob(["*.java"]),
    deps = [
        # Internal dependencies
        "//common:common",
    ],
    native_libraries_deps = [
        "//concept:concept",
        "//logic:logic",
        "//query:query",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb:{pom_version}"],
    visibility = ["//visibility:public"],
)

assemble_files = {
    "//server/conf:logback": "server/conf/logback.xml",
    "//server/conf:logback-debug": "server/conf/logback-debug.xml",
    "//server/conf:typedb-properties": "server/conf/typedb.properties",
    "//server/resources:logo": "server/resources/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}

permissions = {
    "server/conf/typedb.properties": "0755",
    "server/conf/logback.xml": "0755",
    "server/conf/logback-debug.xml": "0755",
}

artifact_repackage(
    name = "console-artifact-jars",
    # Jars produced for all platforms are the same
    srcs = ["@vaticle_typedb_console_artifact_linux//file"],
    files_to_keep = ["console"],
)

assemble_targz(
    name = "assemble-linux-targz",
    targets = ["//server:server-deps-linux", ":console-artifact-jars", "@vaticle_typedb_common//binary:assemble-bash-targz"],
    additional_files = assemble_files,
    permissions = permissions,
    output_filename = "typedb-all-linux",
)

assemble_zip(
    name = "assemble-mac-zip",
    targets = ["//server:server-deps-mac", "//server:server-deps-prod", ":console-artifact-jars", "@vaticle_typedb_common//binary:assemble-bash-targz"],
    additional_files = assemble_files,
    permissions = permissions,
    output_filename = "typedb-all-mac",
)

assemble_zip(
    name = "assemble-windows-zip",
    targets = ["//server:server-deps-windows", ":console-artifact-jars", "@vaticle_typedb_common//binary:assemble-bat-targz"],
    additional_files = assemble_files,
    permissions = permissions,
    output_filename = "typedb-all-windows",
)

deploy_artifact(
    name = "deploy-linux-targz",
    target = ":assemble-linux-targz",
    artifact_group = "vaticle_typedb",
    artifact_name = "typedb-all-linux-{version}.tar.gz",
    release = deployment['artifact.release'],
    snapshot = deployment['artifact.snapshot'],
)

deploy_artifact(
    name = "deploy-mac-zip",
    target = ":assemble-mac-zip",
    artifact_group = "vaticle_typedb",
    artifact_name = "typedb-all-mac-{version}.zip",
    release = deployment['artifact.release'],
    snapshot = deployment['artifact.snapshot'],
)

deploy_artifact(
    name = "deploy-windows-zip",
    target = ":assemble-windows-zip",
    artifact_group = "vaticle_typedb",
    artifact_name = "typedb-all-windows-{version}.zip",
    release = deployment['artifact.release'],
    snapshot = deployment['artifact.snapshot'],
)

assemble_versioned(
    name = "assemble-versioned-all",
    targets = [
        ":assemble-linux-targz",
        ":assemble-mac-zip",
        ":assemble-windows-zip",
        "//server:assemble-linux-targz",
        "//server:assemble-mac-zip",
        "//server:assemble-windows-zip",
    ],
)

assemble_versioned(
    name = "assemble-versioned-mac",
    targets = [":assemble-mac-zip"],
)

checksum(
    name = "checksum-mac",
    archive = ":assemble-versioned-mac",
)

deploy_github(
    name = "deploy-github",
    organisation = deployment_github['github.organisation'],
    repository = deployment_github['github.repository'],
    title = "TypeDB",
    title_append_version = True,
    release_description = "//:RELEASE_TEMPLATE.md",
    archive = ":assemble-versioned-all",
    draft = False
)

deploy_brew(
    name = "deploy-brew",
    snapshot = deployment['brew.snapshot'],
    release = deployment['brew.release'],
    formula = "//config/brew:typedb.rb",
    checksum = "//:checksum-mac",
    version_file = "//:VERSION"
)

assemble_apt(
    name = "assemble-linux-apt",
    package_name = "typedb-all",
    maintainer = "Vaticle <community@vaticle.com>",
    description = "TypeDB (all)",
    depends = [
        "openjdk-11-jre",
        "typedb-server (=%{version})",
        "typedb-console (=%{@vaticle_typedb_console_artifact_linux})",
    ],
    workspace_refs = "@vaticle_typedb_workspace_refs//:refs.json",
)

deploy_apt(
    name = "deploy-apt",
    target = ":assemble-linux-apt",
    snapshot = deployment['apt.snapshot'],
    release = deployment['apt.release'],
)

release_validate_deps(
    name = "release-validate-deps",
    refs = "@vaticle_typedb_workspace_refs//:refs.json",
    tagged_deps = [
        "@vaticle_typeql_lang_java",
        "@vaticle_typedb_common",
        "@vaticle_typedb_protocol",
    ],
    tags = ["manual"]  # in order for bazel test //... to not fail
)

container_image(
    name = "assemble-docker",
    base = "@openjdk_image//image",
    tars = [":assemble-linux-targz"],
    directory = "opt",
    workdir = "/opt/typedb-all-linux",
    ports = ["1729"],
    cmd = ["/opt/typedb-all-linux/typedb", "server"],
    volumes = ["/opt/typedb-all-linux/server/data/"],
    visibility = ["//test:__subpackages__"],
)

container_bundle(
    name = "assemble-docker-bundle",
    images = {
        "{}/{}/{}:{{DOCKER_VERSION}}".format(
            deployment_docker['docker.release'], deployment_docker['docker.organisation'], deployment_docker['docker.repository']
        ): ":assemble-docker",
        "{}/{}/{}:latest".format(
            deployment_docker['docker.release'], deployment_docker['docker.organisation'], deployment_docker['docker.repository']
        ): ":assemble-docker",
    }
)

docker_push(
    name = "deploy-docker",
    bundle = ":assemble-docker-bundle",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", ".grabl/*", "bin/*", ".circleci/*"]),
    exclude = glob(["docs/*", ".circleci/windows/*"]),
    license_type = "agpl",
)

# CI targets that are not declared in any BUILD file, but are called externally
filegroup(
    name = "ci",
    data = [
        "@vaticle_dependencies//library/maven:update",
        "@vaticle_dependencies//tool/checkstyle:test-coverage",
        "@vaticle_dependencies//tool/sonarcloud:code-analysis",
        "@vaticle_dependencies//tool/unuseddeps:unused-deps",
    ],
)

