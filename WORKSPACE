#
# Copyright (C) 2019 Grakn Labs
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

workspace(name = "graknlabs_grakn_core")


###########################
# Load Build Dependencies #
###########################

load("//dependencies/graknlabs:repositories.bzl", "graknlabs_dependencies")
graknlabs_dependencies()

load("@graknlabs_dependencies//builder/antlr:deps.bzl", antlr_deps = "deps")
antlr_deps()
load("@rules_antlr//antlr:deps.bzl", "antlr_dependencies")
antlr_dependencies()

load("@graknlabs_dependencies//builder/bazel:deps.bzl", "bazel_toolchain")
bazel_toolchain()

load("@graknlabs_dependencies//builder/java:deps.bzl", java_deps = "deps")
java_deps()
load("@graknlabs_dependencies//library/maven:rules.bzl", "maven")

load("@graknlabs_dependencies//builder/kotlin:deps.bzl", kotlin_deps = "deps")
kotlin_deps()
load("@io_bazel_rules_kotlin//kotlin:kotlin.bzl", "kotlin_repositories", "kt_register_toolchains")
kotlin_repositories()
kt_register_toolchains()

load("@graknlabs_dependencies//builder/python:deps.bzl", python_deps = "deps")
python_deps()

load("@rules_python//python:pip.bzl", "pip_repositories", "pip3_import")
pip_repositories()

pip3_import(
    name = "graknlabs_dependencies_ci_pip",
    requirements = "@graknlabs_dependencies//tool:requirements.txt",
)

load("@graknlabs_dependencies_ci_pip//:requirements.bzl",
graknlabs_dependencies_ci_pip_install = "pip_install")
graknlabs_dependencies_ci_pip_install()

load("@graknlabs_dependencies//tool/checkstyle:deps.bzl", checkstyle_deps = "deps")
checkstyle_deps()

load("@graknlabs_dependencies//tool/sonarcloud:deps.bzl", "sonarcloud_dependencies")
sonarcloud_dependencies()

load("@graknlabs_dependencies//tool/unuseddeps:deps.bzl", unuseddeps_deps = "deps")
unuseddeps_deps()

load("@graknlabs_dependencies//builder/grpc:deps.bzl", grpc_deps = "deps")
grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl",
com_github_grpc_grpc_deps = "grpc_deps")
com_github_grpc_grpc_deps()

##################################
# Load Distribution Dependencies #
##################################

load("@graknlabs_dependencies//dependencies/graknlabs:repositories.bzl", "graknlabs_bazel_distribution")
graknlabs_bazel_distribution()

load("@graknlabs_bazel_distribution//common:dependencies.bzl", "bazelbuild_rules_pkg")
bazelbuild_rules_pkg()

load("@graknlabs_bazel_distribution//github:dependencies.bzl", "tcnksm_ghr")
tcnksm_ghr()

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")
rules_pkg_dependencies()

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
git_repository(
    name = "io_bazel_skydoc",
    remote = "https://github.com/graknlabs/skydoc.git",
    branch = "experimental-skydoc-allow-dep-on-bazel-tools",
)

load("@io_bazel_skydoc//:setup.bzl", "skydoc_repositories")
skydoc_repositories()

################################
# Load Grakn Labs Dependencies #
################################

load("@graknlabs_dependencies//dependencies/maven:artifacts.bzl",
graknlabs_dependencies_artifacts = "artifacts")

# We don't load Maven artifacts for @graknlabs_common as they are only needed
# if you depend on @graknlabs_common//test/server
load("//dependencies/graknlabs:repositories.bzl", "graknlabs_common")
graknlabs_common()

load("//dependencies/graknlabs:repositories.bzl", "graknlabs_graql")
graknlabs_graql()
load("@graknlabs_graql//dependencies/maven:artifacts.bzl",
graknlabs_graql_artifacts = "artifacts")

load("//dependencies/graknlabs:repositories.bzl", "graknlabs_protocol")
graknlabs_protocol()
load("@graknlabs_protocol//dependencies/maven:artifacts.bzl",
graknlabs_protocol_artifacts = "artifacts")

load("//dependencies/graknlabs:repositories.bzl", "graknlabs_grabl_tracing")
graknlabs_grabl_tracing()
load("@graknlabs_grabl_tracing//dependencies/maven:artifacts.bzl",
graknlabs_grabl_tracing_artifacts = "artifacts")

load("//dependencies/graknlabs:repositories.bzl", "graknlabs_behaviour")
graknlabs_behaviour()

load("//dependencies/maven:artifacts.bzl", graknlabs_grakn_core_artifacts = "artifacts")

GRAKN_CORE_OVERRIDES = {
    "io.netty:netty-all": "4.1.38.Final",
    "io.netty:netty-buffer": "4.1.38.Final",
    "io.netty:netty-codec": "4.1.38.Final",
    "io.netty:netty-codec-http2": "4.1.38.Final",
    "io.netty:netty-codec-http": "4.1.38.Final",
    "io.netty:netty-codec-socks": "4.1.38.Final",
    "io.netty:netty-common": "4.1.38.Final",
    "io.netty:netty-handler": "4.1.38.Final",
    "io.netty:netty-handler-proxy": "4.1.38.Final",
    "io.netty:netty-resolver": "4.1.38.Final",
    "io.netty:netty-transport": "4.1.38.Final",
}

maven(
    graknlabs_dependencies_artifacts +
    graknlabs_grabl_tracing_artifacts +
    graknlabs_graql_artifacts +
    graknlabs_grakn_core_artifacts,
    GRAKN_CORE_OVERRIDES
)

###############################################
# Create @graknlabs_grakn_core_workspace_refs #
###############################################

load("@graknlabs_bazel_distribution//common:rules.bzl", "workspace_refs")
workspace_refs(name = "graknlabs_grakn_core_workspace_refs")