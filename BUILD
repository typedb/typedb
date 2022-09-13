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
load("@vaticle_dependencies//tool/release/deps:rules.bzl", "release_validate_deps")
load("@io_bazel_rules_docker//container:image.bzl", docker_container_image = "container_image")
load("@io_bazel_rules_docker//container:container.bzl", docker_container_push = "container_push")

exports_files(
    ["VERSION", "deployment.bzl", "RELEASE_TEMPLATE.md", "LICENSE", "README.md"],
)

native_java_libraries(
    name = "typedb",
    srcs = glob(["*.java"]),
    native_libraries_deps = [
        # Internal dependencies
        "//common:common",
        "//concept:concept",
        "//logic:logic",
        "//query:query",
    ],
    deps = [
        # Vaticle Dependencies
        "@vaticle_typedb_common//:common",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb:{pom_version}"],
    visibility = ["//visibility:public"],
)

assemble_files = {
    "//server:config": "server/conf/config.yml",
    "//server/resources:logo": "server/resources/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}

empty_directories = [
    "server/data"
]

permissions = {
    "server/conf/config.yml" : "0755",
    "server/data" : "0755",
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
    empty_directories = empty_directories,
    permissions = permissions,
    output_filename = "typedb-all-linux",
)

assemble_zip(
    name = "assemble-mac-zip",
    targets = ["//server:server-deps-mac", "//server:server-deps-prod", ":console-artifact-jars", "@vaticle_typedb_common//binary:assemble-bash-targz"],
    additional_files = assemble_files,
    empty_directories = empty_directories,
    permissions = permissions,
    output_filename = "typedb-all-mac",
)

assemble_zip(
    name = "assemble-windows-zip",
    targets = ["//server:server-deps-windows", ":console-artifact-jars", "@vaticle_typedb_common//binary:assemble-bat-targz"],
    additional_files = assemble_files,
    empty_directories = empty_directories,
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
        "@vaticle_typeql",
        "@vaticle_typedb_common",
        "@vaticle_typedb_protocol",
    ],
    tags = ["manual"]  # in order for bazel test //... to not fail
)

docker_container_image(
    name = "assemble-docker",
    base = "@vaticle_ubuntu_image//image",
    tars = [":assemble-linux-targz"],
    directory = "opt",
    workdir = "/opt/typedb-all-linux",
    ports = ["1729"],
    env = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    },
    cmd = ["/opt/typedb-all-linux/typedb", "server"],
    volumes = ["/opt/typedb-all-linux/server/data/"],
    visibility = ["//test:__subpackages__"],
)

docker_container_push(
    name = "deploy-docker-latest",
    image = ":assemble-docker",
    format = "Docker",
    registry = deployment_docker["docker.release"],
    repository = "{}/{}".format(
        deployment_docker["docker.organisation"],
        deployment_docker["docker.repository"],
    ),
    tag = "latest"
)

docker_container_push(
    name = "deploy-docker-versioned",
    image = ":assemble-docker",
    format = "Docker",
    registry = deployment_docker["docker.release"],
    repository = "{}/{}".format(
        deployment_docker["docker.organisation"],
        deployment_docker["docker.repository"],
    ),
    tag_file = ":VERSION",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", ".grabl/*", "bin/*", ".circleci/*"]),
    exclude = glob([
        "*.md",
        ".circleci/windows/*",
        "docs/*",
    ]) + [
        ".bazelversion",
        "LICENSE",
        "VERSION",
        "typedb.iml",  # TODO remove when checkstyle_test has .gitignore support
    ],
    license_type = "agpl-header",
)

checkstyle_test(
    name = "checkstyle-license",
    include = ["LICENSE"],
    license_type = "agpl-fulltext",
)

# CI targets that are not declared in any BUILD file, but are called externally
filegroup(
    name = "ci",
    data = [
        "@vaticle_dependencies//factory/analysis:dependency-analysis",
        "@vaticle_dependencies//library/maven:update",
        "@vaticle_dependencies//tool/release/notes:create",
        "@vaticle_dependencies//tool/checkstyle:test-coverage",
        "@vaticle_dependencies//tool/sonarcloud:code-analysis",
        "@vaticle_dependencies//tool/unuseddeps:unused-deps",
    ],
)

