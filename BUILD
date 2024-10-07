# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("//:deployment.bzl", deployment_docker = "deployment", deployment_github = "deployment")
load("@vaticle_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@vaticle_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@vaticle_bazel_distribution//brew:rules.bzl", "deploy_brew")
load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_versioned", "assemble_zip", "checksum")
load("@vaticle_bazel_distribution//github:rules.bzl", "deploy_github")
load("@vaticle_bazel_distribution//common/targz:rules.bzl", "targz_edit")
load("@vaticle_bazel_distribution//platform:constraints.bzl", "constraint_linux_arm64", "constraint_linux_x86_64",
     "constraint_mac_arm64", "constraint_mac_x86_64", "constraint_win_x86_64")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//distribution/artifact:rules.bzl", "artifact_repackage")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//tool/release/deps:rules.bzl", "release_validate_deps")
load("@io_bazel_rules_docker//container:image.bzl", docker_container_image = "container_image")
load("@io_bazel_rules_docker//container:container.bzl", docker_container_push = "container_push")


exports_files(
    [
        "VERSION",
        "deployment.bzl",
        "LICENSE",
        "README.md",
    ],
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
    tags = ["maven_coordinates=com.vaticle.typedb:typedb:{pom_version}"],
    visibility = ["//visibility:public"],
    deps = [
        # Vaticle Dependencies
        "@vaticle_typeql//common/java:common",
    ],
)

assemble_files = {
    "//server/parameters/config": "server/conf/config.yml",
    "//server/resources:logo": "server/resources/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}

empty_directories = [
    "server/data",
]

permissions = {
    "server/conf/config.yml": "0755",
    "server/data": "0755",
}

artifact_repackage(
    name = "console-artifact-jars-linux-arm64",
    srcs = ["@vaticle_typedb_console_artifact_linux-arm64//file"],
    files_to_keep = ["console"],
)

assemble_targz(
    name = "assemble-linux-arm64-targz",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-linux-arm64",
    permissions = permissions,
    targets = [
        ":console-artifact-jars-linux-arm64",
        "//server:server-deps-linux-arm64",
        "//binary:assemble-bash-targz",
    ],
)

artifact_repackage(
    name = "console-artifact-jars-linux-x86_64",
    srcs = ["@vaticle_typedb_console_artifact_linux-x86_64//file"],
    files_to_keep = ["console"],
)

assemble_targz(
    name = "assemble-linux-x86_64-targz",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-linux-x86_64",
    permissions = permissions,
    targets = [
        ":console-artifact-jars-linux-x86_64",
        "//server:server-deps-linux-x86_64",
        "//binary:assemble-bash-targz",
    ],
)

artifact_repackage(
    name = "console-artifact-jars-mac-arm64",
    srcs = ["@vaticle_typedb_console_artifact_mac-arm64//file"],
    files_to_keep = ["console"],
)

assemble_zip(
    name = "assemble-mac-arm64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-mac-arm64",
    permissions = permissions,
    targets = [
        "//server:server-deps-mac-arm64",
        ":console-artifact-jars-mac-arm64",
        "//binary:assemble-bash-targz",
    ],
)

artifact_repackage(
    name = "console-artifact-jars-mac-x86_64",
    srcs = ["@vaticle_typedb_console_artifact_mac-x86_64//file"],
    files_to_keep = ["console"],
)

assemble_zip(
    name = "assemble-mac-x86_64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-mac-x86_64",
    permissions = permissions,
    targets = [
        "//server:server-deps-mac-x86_64",
        ":console-artifact-jars-mac-x86_64",
        "//binary:assemble-bash-targz",
    ],
)

artifact_repackage(
    name = "console-artifact-jars-windows-x86_64",
    srcs = ["@vaticle_typedb_console_artifact_windows-x86_64//file"],
    files_to_keep = ["console"],
)

assemble_zip(
    name = "assemble-windows-x86_64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-windows-x86_64",
    permissions = permissions,
    targets = [
        "//server:server-deps-windows-x86_64",
        ":console-artifact-jars-windows-x86_64",
        "//binary:assemble-bat-targz",
    ],
)

deploy_artifact(
    name = "deploy-linux-arm64-targz",
    artifact_group = "typedb-all-linux-arm64",
    artifact_name = "typedb-all-linux-arm64-{version}.tar.gz",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-linux-arm64-targz",
)

deploy_artifact(
    name = "deploy-linux-x86_64-targz",
    artifact_group = "typedb-all-linux-x86_64",
    artifact_name = "typedb-all-linux-x86_64-{version}.tar.gz",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-linux-x86_64-targz",
)

deploy_artifact(
    name = "deploy-mac-arm64-zip",
    artifact_group = "typedb-all-mac-arm64",
    artifact_name = "typedb-all-mac-arm64-{version}.zip",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-mac-arm64-zip",
)

deploy_artifact(
    name = "deploy-mac-x86_64-zip",
    artifact_group = "typedb-all-mac-x86_64",
    artifact_name = "typedb-all-mac-x86_64-{version}.zip",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-mac-x86_64-zip",
)

deploy_artifact(
    name = "deploy-windows-x86_64-zip",
    artifact_group = "typedb-all-windows-x86_64",
    artifact_name = "typedb-all-windows-x86_64-{version}.zip",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-windows-x86_64-zip",
)

assemble_versioned(
    name = "assemble-versioned-all",
    targets = [
        ":assemble-linux-arm64-targz",
        ":assemble-linux-x86_64-targz",
        ":assemble-mac-arm64-zip",
        ":assemble-mac-x86_64-zip",
        ":assemble-windows-x86_64-zip",
        "//server:assemble-linux-arm64-targz",
        "//server:assemble-linux-x86_64-targz",
        "//server:assemble-mac-arm64-zip",
        "//server:assemble-mac-x86_64-zip",
        "//server:assemble-windows-x86_64-zip",
    ],
)

assemble_versioned(
    name = "assemble-versioned-mac",
    targets = [":assemble-mac-arm64-zip"],
)

checksum(
    name = "checksum-mac-arm64",
    archive = ":assemble-mac-arm64-zip",
)

checksum(
    name = "checksum-mac-x86_64",
    archive = ":assemble-mac-x86_64-zip",
)

deploy_github(
    name = "deploy-github",
    archive = ":assemble-versioned-all",
    draft = False,
    organisation = deployment_github["github.organisation"],
    release_description = "//:RELEASE_NOTES_LATEST.md",
    repository = deployment_github["github.repository"],
    title = "TypeDB",
    title_append_version = True,
)

deploy_brew(
    name = "deploy-brew",
    file_substitutions = {
        "//:checksum-mac-arm64": "{sha256-arm64}",
        "//:checksum-mac-x86_64": "{sha256-x86_64}",
    },
    formula = "//config/brew:typedb.rb",
    release = deployment["brew"]["release"],
    snapshot = deployment["brew"]["snapshot"],
    version_file = "//:VERSION",
)

apt_depends = ["default-jre"]

apt_installation_dir = "/opt/typedb/core/"

apt_empty_dirs = [
    "/var/log/typedb/",
    "/opt/typedb/core/server/lib/",
    "/var/lib/typedb/core/data/",
    "/opt/typedb/core/console/lib/",
]

apt_symlinks = {
    "/opt/typedb/core/server/data": "/var/lib/typedb/core/data/",
    "/usr/local/bin/typedb": "/opt/typedb/core/typedb",
    "/opt/typedb/core/server/logs": "/var/log/typedb/",
    "/usr/lib/systemd/system/typedb.service": "/opt/typedb/core/typedb.service",
}

targz_edit(
    name = "console-artifact-native-x86_64.tar.gz",
    src = "@vaticle_typedb_console_artifact_linux-x86_64//file",
    exclude_globs = ["typedb"],
    strip_components = 1,
)

assemble_apt(
    name = "assemble-linux-x86_64-apt",
    package_name = "typedb",
    architecture = "amd64",
    archives = [
        "//server:server-deps-linux-x86_64",
        ":console-artifact-native-x86_64.tar.gz",
        "//binary:assemble-bash-targz",
        "//binary:assemble-apt-targz",
    ],
    depends = apt_depends,
    description = "TypeDB",
    empty_dirs = apt_empty_dirs,
    empty_dirs_permission = "0777",
    files = assemble_files,
    installation_dir = apt_installation_dir,
    maintainer = "TypeDB Community <community@typedb.com>",
    symlinks = apt_symlinks,
    workspace_refs = "@vaticle_typedb_workspace_refs//:refs.json",
)

deploy_apt(
    name = "deploy-apt-x86_64",
    release = deployment["apt"]["release"]["upload"],
    snapshot = deployment["apt"]["snapshot"]["upload"],
    target = ":assemble-linux-x86_64-apt",
)

targz_edit(
    name = "console-artifact-native-arm64.tar.gz",
    src = "@vaticle_typedb_console_artifact_linux-arm64//file",
    exclude_globs = ["typedb"],
    strip_components = 1,
)

assemble_apt(
    name = "assemble-linux-arm64-apt",
    package_name = "typedb",
    architecture = "arm64",
    archives = [
        "//server:server-deps-linux-arm64",
        ":console-artifact-native-arm64.tar.gz",
        "//binary:assemble-bash-targz",
        "//binary:assemble-apt-targz",
    ],
    depends = apt_depends,
    description = "TypeDB",
    empty_dirs = apt_empty_dirs,
    empty_dirs_permission = "0777",
    files = assemble_files,
    installation_dir = apt_installation_dir,
    maintainer = "TypeDB Community <community@typedb.com>",
    symlinks = apt_symlinks,
    workspace_refs = "@vaticle_typedb_workspace_refs//:refs.json",
)

deploy_apt(
    name = "deploy-apt-arm64",
    release = deployment["apt"]["release"]["upload"],
    snapshot = deployment["apt"]["snapshot"]["upload"],
    target = ":assemble-linux-arm64-apt",
)

release_validate_deps(
    name = "release-validate-deps",
    refs = "@vaticle_typedb_workspace_refs//:refs.json",
    tagged_deps = [
        "@vaticle_typeql",
        "@vaticle_typedb_protocol",
    ],
    tags = ["manual"],  # in order for bazel test //... to not fail
    version_file = "VERSION",
)

# docker
docker_container_image(
    name = "assemble-docker-x86_64",
    operating_system = "linux",
    architecture = "amd64",
    base = "@ubuntu-22.04-x86_64//image",
    cmd = ["/opt/typedb-all-linux-x86_64/typedb", "server"],
    directory = "opt",
    env = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    },
    ports = ["1729"],
    tars = [":assemble-linux-x86_64-targz"],
    visibility = ["//test:__subpackages__"],
    volumes = ["/opt/typedb-all-linux-x86_64/server/data/"],
    workdir = "/opt/typedb-all-linux-x86_64",
)

docker_container_image(
    name = "assemble-docker-arm64",
    operating_system = "linux",
    architecture = "arm64",
    base = "@ubuntu-22.04-arm64//image",
    cmd = ["/opt/typedb-all-linux-arm64/typedb", "server"],
    directory = "opt",
    env = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    },
    ports = ["1729"],
    tars = [":assemble-linux-arm64-targz"],
    visibility = ["//test:__subpackages__"],
    volumes = ["/opt/typedb-all-linux-arm64/server/data/"],
    workdir = "/opt/typedb-all-linux-arm64",
)

docker_container_push(
    name = "deploy-docker-release-x86_64",
    format = "Docker",
    image = ":assemble-docker-x86_64",
    registry = deployment_docker["docker.index"],
    repository = "{}/{}".format(
        deployment_docker["docker.organisation"],
        deployment_docker["docker.release.repository"],
    ),
    tag_file = "//docker:version-x86_64",
)

docker_container_push(
    name = "deploy-docker-release-arm64",
    format = "Docker",
    image = ":assemble-docker-arm64",
    registry = deployment_docker["docker.index"],
    repository = "{}/{}".format(
        deployment_docker["docker.organisation"],
        deployment_docker["docker.release.repository"],
    ),
    tag_file = "//docker:version-arm64",
)
#
#docker_container_push(
#    name = "deploy-docker-release-overwrite-latest-tag",
#    format = "Docker",
#    image = ":assemble-docker",
#    registry = deployment_docker["docker.index"],
#    repository = "{}/{}".format(
#        deployment_docker["docker.organisation"],
#        deployment_docker["docker.release.repository"],
#    ),
#    tag = "latest",
#)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        ".factory/*",
        "bin/*",
        ".circleci/*",
    ]),
    exclude = glob([
        "*.md",
        ".circleci/windows/*",
        "docs/*",
        "tools/**",
    ]) + [
        ".bazelversion",
        ".bazel-remote-cache.rc",
        ".bazel-cache-credential.json",
        "LICENSE",
        "VERSION",
    ],
    license_type = "mpl-header",
)

checkstyle_test(
    name = "checkstyle-license",
    include = ["LICENSE"],
    license_type = "mpl-fulltext",
)

# CI targets that are not declared in any BUILD file, but are called externally
filegroup(
    name = "ci",
    data = [
        "@vaticle_dependencies//factory/analysis:dependency-analysis",
        "@vaticle_dependencies//library/maven:update",
        "@vaticle_dependencies//tool/bazelinstall:remote_cache_setup.sh",
        "@vaticle_dependencies//tool/checkstyle:test-coverage",
        "@vaticle_dependencies//tool/release/notes:create",
        "@vaticle_dependencies//tool/sync:dependencies",
        "@vaticle_dependencies//tool/unuseddeps:unused-deps",
    ],
)
