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


################################
# Load @graknlabs_dependencies #
################################

load("//dependencies/graknlabs:repositories.bzl", "graknlabs_dependencies")
graknlabs_dependencies()

# Load Bazel
load("@graknlabs_dependencies//builder/bazel:deps.bzl","bazel_common", "bazel_deps", "bazel_toolchain")
bazel_common()
bazel_deps()
bazel_toolchain()

# Load Java
load("@graknlabs_dependencies//builder/java:deps.bzl", java_deps = "deps")
java_deps()
load("@graknlabs_dependencies//library/maven:rules.bzl", "maven")

# Load Kotlin
load("@graknlabs_dependencies//builder/kotlin:deps.bzl", kotlin_deps = "deps")
kotlin_deps()
load("@io_bazel_rules_kotlin//kotlin:kotlin.bzl", "kotlin_repositories", "kt_register_toolchains")
kotlin_repositories()
kt_register_toolchains()

# Load Checkstyle
load("@graknlabs_dependencies//tool/checkstyle:deps.bzl", checkstyle_deps = "deps")
checkstyle_deps()

# Load Sonarcloud
load("@graknlabs_dependencies//tool/sonarcloud:deps.bzl", "sonarcloud_dependencies")
sonarcloud_dependencies()

# Load Unused Deps
load("@graknlabs_dependencies//tool/unuseddeps:deps.bzl", unuseddeps_deps = "deps")
unuseddeps_deps()

##########################
# Load GRPC dependencies #
##########################

load("@graknlabs_dependencies//builder/grpc:deps.bzl", grpc_deps = "deps")
grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl",
com_github_grpc_grpc_deps = "grpc_deps")
com_github_grpc_grpc_deps()

#####################################################################
# Load @graknlabs_bazel_distribution from (@graknlabs_dependencies) #
#####################################################################
load("@graknlabs_dependencies//distribution:deps.bzl", distribution_deps = "deps")
distribution_deps()

##########################
# Load @graknlabs_common #
##########################
load("//dependencies/graknlabs:repositories.bzl", "graknlabs_common")
graknlabs_common()

#################################
# Load @graknlabs_grabl_tracing #
#################################
load("//dependencies/graknlabs:repositories.bzl", "graknlabs_grabl_tracing")
graknlabs_grabl_tracing()

load("@graknlabs_grabl_tracing//dependencies/maven:artifacts.bzl", graknlabs_grabl_tracing_artifacts = "artifacts")

##############################
# Load @graknlabs_verification #
##############################
load("//dependencies/graknlabs:repositories.bzl", "graknlabs_verification")
graknlabs_verification()

###########################
# Load Maven dependencies #
###########################
load("//dependencies/maven:artifacts.bzl", graknlabs_grakn_core_artifacts = "artifacts")
maven(
    graknlabs_grakn_core_artifacts +
    graknlabs_grabl_tracing_artifacts
)
