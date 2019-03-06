#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
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

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")


################################
# Load Grakn Labs Dependencies #
################################

git_repository(
    name = "graknlabs_graql",
    remote = "https://github.com/graknlabs/graql",
    commit = "33b53ca88871a4efdd3d4e17e7e40e2317c3778b", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_graql
)

git_repository(
    name = "graknlabs_client_java",
    remote = "https://github.com/graknlabs/client-java",
    commit = "c2485b7321bcdff2475d0e3ae0cb7108b8d44a75",
)

git_repository(
    name = "graknlabs_build_tools",
    remote = "https://github.com/lolski/build-tools",
    commit = "c7bce0ed4d84c4d7195dfadd646349d5f994a25e", # sync-marker: do not remove this comment, this is used for sync-dependencies by @graknlabs_build_tools
)

load("@graknlabs_build_tools//distribution:dependencies.bzl", "graknlabs_bazel_distribution")
graknlabs_bazel_distribution()


####################
# Load Build Tools #
####################

load("@graknlabs_build_tools//bazel:dependencies.bzl", "bazel_common", "bazel_deps", "bazel_toolchain", "bazel_rules_docker")
bazel_common()
bazel_deps()
bazel_toolchain()
bazel_rules_docker()


load("@graknlabs_build_tools//bazel:dependencies.bzl", "buildifier", "buildozer", "unused_deps")
buildifier()
buildozer()
unused_deps()

load("@graknlabs_build_tools//checkstyle:dependencies.bzl", "checkstyle_dependencies")
checkstyle_dependencies()


#####################################
# Load Java dependencies from Maven #
#####################################

load("//dependencies/maven:dependencies.bzl", "maven_dependencies")
maven_dependencies()


###########################
# Load Graql dependencies #
###########################

# Load ANTLR dependencies for Bazel
load("@graknlabs_graql//dependencies/compilers:dependencies.bzl", "antlr_dependencies")
antlr_dependencies()

# Load ANTLR dependencies for ANTLR programs
load("@rules_antlr//antlr:deps.bzl", "antlr_dependencies")
antlr_dependencies()

load("@graknlabs_graql//dependencies/maven:dependencies.bzl", graql_dependencies = "maven_dependencies")
graql_dependencies()


#######################################
# Load compiler dependencies for GRPC #
#######################################

# Load GRPC dependencies
load("//dependencies/compilers:dependencies.bzl", "grpc_dependencies")
grpc_dependencies()

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", com_github_grpc_grpc_bazel_grpc_deps = "grpc_deps")
com_github_grpc_grpc_bazel_grpc_deps()

# Load GRPC Java dependencies
load("@stackb_rules_proto//java:deps.bzl", "java_grpc_compile")
java_grpc_compile()


##################################
# Load Distribution Dependencies #
##################################

load("@graknlabs_bazel_distribution//github:dependencies.bzl", "github_dependencies_for_deployment")
github_dependencies_for_deployment()

# Why does it break when we move thie declaration after loading tools_dependencies?
load("@com_github_google_bazel_common//:workspace_defs.bzl", "google_common_workspace_rules")
google_common_workspace_rules()

load("@io_bazel_rules_docker//repositories:repositories.bzl", container_repositories = "repositories")
container_repositories()

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")
container_pull(
  name = "openjdk_image",
  registry = "index.docker.io",
  repository = "library/openjdk",
  tag = "8"
)
