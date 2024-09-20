# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

workspace(name = "vaticle_typedb")

################################
# Load @vaticle_dependencies #
################################

load("//dependencies/vaticle:repositories.bzl", "vaticle_dependencies")
vaticle_dependencies()

# Load //builder/bazel for RBE
load("@vaticle_dependencies//builder/bazel:deps.bzl", "bazel_toolchain")
bazel_toolchain()

# Load //builder/java
load("@vaticle_dependencies//builder/java:deps.bzl", java_deps = "deps")
java_deps()
load("@vaticle_dependencies//library/maven:rules.bzl", "maven")

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")
rules_jvm_external_deps()

# Load //builder/kotlin
load("@vaticle_dependencies//builder/kotlin:deps.bzl", kotlin_deps = "deps")
kotlin_deps()
load("@io_bazel_rules_kotlin//kotlin:repositories.bzl", "kotlin_repositories")
kotlin_repositories()
load("@io_bazel_rules_kotlin//kotlin:core.bzl", "kt_register_toolchains")
kt_register_toolchains()

# Load //builder/proto_grpc
load("@vaticle_dependencies//builder/proto_grpc:deps.bzl", proto_grpc_deps = "deps")
proto_grpc_deps()

load("@rules_proto_grpc//:repositories.bzl", "rules_proto_grpc_repos", "rules_proto_grpc_toolchains")
rules_proto_grpc_toolchains()
rules_proto_grpc_repos()

# Load //tool/common
load("@vaticle_dependencies//tool/common:deps.bzl", "vaticle_dependencies_ci_pip",
    vaticle_dependencies_tool_maven_artifacts = "maven_artifacts")
vaticle_dependencies_ci_pip()

# Load //tool/checkstyle
load("@vaticle_dependencies//tool/checkstyle:deps.bzl", checkstyle_deps = "deps")
checkstyle_deps()

# Load //tool/unuseddeps
load("@vaticle_dependencies//tool/unuseddeps:deps.bzl", unuseddeps_deps = "deps")
unuseddeps_deps()

# Load //builder/rust
load("@vaticle_dependencies//builder/rust:deps.bzl", rust_deps = "deps")
rust_deps()

load("@rules_rust//rust:repositories.bzl", "rules_rust_dependencies", "rust_register_toolchains", "rust_analyzer_toolchain_tools_repository")
rules_rust_dependencies()
load("@rules_rust//tools/rust_analyzer:deps.bzl", "rust_analyzer_dependencies")
rust_analyzer_dependencies()
load("@rules_rust//rust:defs.bzl", "rust_common")
rust_register_toolchains(
    edition = "2021",
    extra_target_triples = [
        "aarch64-apple-darwin",
        "aarch64-unknown-linux-gnu",
        "x86_64-apple-darwin",
        "x86_64-pc-windows-msvc",
        "x86_64-unknown-linux-gnu",
    ],
    rust_analyzer_version = "1.80.1",
    versions = ["1.80.1"],
)

rust_analyzer_toolchain_tools_repository(
    name = "rust_analyzer_toolchain_tools",
    version = "1.80.1"
)

load("@vaticle_dependencies//library/crates:crates.bzl", "fetch_crates")
fetch_crates()
load("@crates//:defs.bzl", "crate_repositories")
crate_repositories()

######################################
# Load @vaticle_bazel_distribution #
######################################

load("//dependencies/vaticle:repositories.bzl", "vaticle_bazel_distribution")
vaticle_bazel_distribution()

# Load @vaticle_bazel_distribution_uploader
load("@vaticle_bazel_distribution//common/uploader:deps.bzl", uploader_deps = "deps")
uploader_deps()
load("@vaticle_bazel_distribution_uploader//:requirements.bzl", install_uploader_deps = "install_deps")
install_uploader_deps()

# Load //common
load("@vaticle_bazel_distribution//common:deps.bzl", "rules_pkg")
rules_pkg()
load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")
rules_pkg_dependencies()

# Load //github
load("@vaticle_bazel_distribution//github:deps.bzl", github_deps = "deps")
github_deps()

# Load //pip
load("@vaticle_bazel_distribution//pip:deps.bzl", pip_deps = "deps")
pip_deps()

###################################################
# Load @vaticle_dependencies//distribution/docker #
###################################################

# must be loaded after `vaticle_bazel_distribution` to ensure
# `rules_pkg` is correctly patched (bazel-distribution #251)

# Load //distribution/docker
load("@vaticle_dependencies//distribution/docker:deps.bzl", docker_deps = "deps")
docker_deps()

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")
go_rules_dependencies()
go_register_toolchains(version = "1.18.3")
gazelle_dependencies()

load("@io_bazel_rules_docker//repositories:repositories.bzl", bazel_rules_docker_repositories = "repositories")
bazel_rules_docker_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", bazel_rules_docker_container_deps = "deps")
bazel_rules_docker_container_deps()

load("//docker:images.bzl", docker_base_images = "base_images")
docker_base_images()

#####################################
# Load @vaticle/typedb dependencies #
#####################################

# We don't load Maven artifacts for @vaticle_typedb_common as they are only needed
# if you depend on @vaticle_typedb_common//test/server
load("//dependencies/vaticle:repositories.bzl", "vaticle_typedb_common", "vaticle_typeql", "vaticle_typedb_protocol", "vaticle_typedb_behaviour")
vaticle_typedb_behaviour()
load("@vaticle_dependencies//tool/common:deps.bzl", "vaticle_dependencies_ci_pip", vaticle_dependencies_tool_maven_artifacts = "maven_artifacts")
vaticle_typedb_common()

vaticle_typeql()

vaticle_typedb_protocol()

#load("//dependencies/vaticle:artifacts.bzl", "vaticle_typedb_console_artifact")
#vaticle_typedb_console_artifact()

############################
# Load @maven dependencies #
############################
load("@vaticle_dependencies//library/maven:rules.bzl", "maven")
maven(
    vaticle_dependencies_tool_maven_artifacts
)

###############################################
# Create @vaticle_typedb_workspace_refs #
###############################################

load("@vaticle_bazel_distribution//common:rules.bzl", "workspace_refs")
workspace_refs(name = "vaticle_typedb_workspace_refs")
