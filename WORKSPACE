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

workspace(name = "graknlabs_grakn_core")


################################
# Load Grakn Labs dependencies #
################################

load(
    "//dependencies/graknlabs:dependencies.bzl",
    "graknlabs_build_tools",
    "graknlabs_common",
    "graknlabs_graql",
    "graknlabs_protocol",
    "graknlabs_client_java",
    "graknlabs_console",
    "graknlabs_benchmark",
    "graknlabs_simulation",
    "graknlabs_verification",
)
graknlabs_build_tools()
graknlabs_common()
graknlabs_graql()
graknlabs_protocol()
graknlabs_client_java()
graknlabs_console()
graknlabs_benchmark()
graknlabs_simulation()
graknlabs_verification()

load("@graknlabs_build_tools//distribution:dependencies.bzl", "graknlabs_bazel_distribution")
graknlabs_bazel_distribution()

load("@graknlabs_build_tools//unused_deps:dependencies.bzl", "unused_deps_dependencies")
unused_deps_dependencies()

###########################
# Load Bazel dependencies #
###########################

load("@graknlabs_build_tools//bazel:dependencies.bzl", "bazel_common", "bazel_deps", "bazel_toolchain")
bazel_common()
bazel_deps()
bazel_toolchain()


#################################
# Load Build Tools dependencies #
#################################

load("@graknlabs_build_tools//checkstyle:dependencies.bzl", "checkstyle_dependencies")
checkstyle_dependencies()

load("@graknlabs_build_tools//sonarcloud:dependencies.bzl", "sonarcloud_dependencies")
sonarcloud_dependencies()

load("@graknlabs_build_tools//bazel:dependencies.bzl", "bazel_rules_python")
bazel_rules_python()

load("@io_bazel_rules_python//python:pip.bzl", "pip_repositories", "pip_import")
pip_repositories()

pip_import(
    name = "graknlabs_build_tools_ci_pip",
    requirements = "@graknlabs_build_tools//ci:requirements.txt",
)

load("@graknlabs_build_tools_ci_pip//:requirements.bzl",
graknlabs_build_tools_ci_pip_install = "pip_install")
graknlabs_build_tools_ci_pip_install()


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

load("@graknlabs_graql//dependencies/maven:dependencies.bzl",
graknlabs_graql_maven_dependencies = "maven_dependencies")
graknlabs_graql_maven_dependencies()


###############################
# Load Benchmark dependencies #
###############################
load("@graknlabs_benchmark//dependencies/maven:dependencies.bzl",
graknlabs_benchmark_maven_dependencies = "maven_dependencies")
graknlabs_benchmark_maven_dependencies()


#######################################
# Load compiler dependencies for GRPC #
#######################################

load("@graknlabs_build_tools//grpc:dependencies.bzl", "grpc_dependencies")
grpc_dependencies()

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl",
com_github_grpc_grpc_deps = "grpc_deps")
com_github_grpc_grpc_deps()

load("@stackb_rules_proto//java:deps.bzl", "java_grpc_compile")
java_grpc_compile()


##################################
# Load Distribution dependencies #
##################################

load("@graknlabs_bazel_distribution//github:dependencies.bzl", "tcnksm_ghr")
tcnksm_ghr()

load("@graknlabs_bazel_distribution//common:dependencies.bzl", "bazelbuild_rules_pkg")
bazelbuild_rules_pkg()

load("@graknlabs_build_tools//bazel:dependencies.bzl", "bazel_rules_docker")
bazel_rules_docker()

load("@io_bazel_rules_docker//repositories:repositories.bzl",
bazel_rules_docker_repositories = "repositories")
bazel_rules_docker_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", bazel_rules_docker_container_deps = "deps")
bazel_rules_docker_container_deps()

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")
container_pull(
  name = "openjdk_image",
  registry = "index.docker.io",
  repository = "library/openjdk",
  tag = "8"
)


################################
# Load Simulation dependencies #
################################

load("@graknlabs_simulation//dependencies/maven:dependencies.bzl",
graknlabs_simulation_dependencies = "maven_dependencies")
graknlabs_simulation_dependencies()

load("@graknlabs_simulation//dependencies/graknlabs:dependencies.bzl", "graknlabs_grabl_tracing")
graknlabs_grabl_tracing()


#####################################
# Load Bazel common workspace rules #
#####################################

# TODO: Figure out why this cannot be loaded at earlier at the top of the file
load("@com_github_google_bazel_common//:workspace_defs.bzl", "google_common_workspace_rules")
google_common_workspace_rules()


# Generate a JSON document of commit hashes of all external workspace dependencies
load("@graknlabs_bazel_distribution//common:rules.bzl", "workspace_refs")
workspace_refs(
    name = "graknlabs_grakn_core_workspace_refs"
)
