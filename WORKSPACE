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

workspace(name = "grakn_core")


####################
# Load Build Tools #
####################

# Load additional build tools, such bazel-deps and unused-deps
load("//dependencies/tools:dependencies.bzl", "tools_dependencies")
tools_dependencies()


#####################################
# Load Java dependencies from Maven #
#####################################

load("//dependencies/maven:dependencies.bzl", maven_dependencies_for_build = "maven_dependencies")
maven_dependencies_for_build()


######################################
# Load Python dependencies from PyPI #
######################################

# Load Python depdendencies for Bazel
load("//dependencies/pip:dependencies.bzl", "python_dependencies",)
python_dependencies()

load("@io_bazel_rules_python//python:pip.bzl", "pip_repositories", "pip_import")
pip_repositories()

# Load PyPI dependencies for Python programs
pip_import(
    name = "pypi_dependencies",
    requirements = "//dependencies/pip:requirements.txt",
)
load("@pypi_dependencies//:requirements.bzl", "pip_install")
pip_install()


######################################
# Load Node.js dependencies from NPM #
######################################

# Load Node.js depdendencies for Bazel
load("//dependencies/npm:dependencies.bzl", "node_dependencies")
node_dependencies()

load("@build_bazel_rules_nodejs//:package.bzl", "rules_nodejs_dependencies")
rules_nodejs_dependencies()

# Load NPM dependencies for Node.js programs
load("@build_bazel_rules_nodejs//:defs.bzl", "node_repositories", "npm_install")
node_repositories(package_json = ["//client-nodejs:package.json"])
npm_install(
    name = "nodejs_dependencies",
    package_json = "//client-nodejs:package.json",
    package_lock_json = "//client-nodejs:package-lock.json",
)

node_repositories(package_json = ["//workbase:package.json"])

########################################
# Load compiler dependencies for ANTLR #
########################################

# Load ANTLR dependencies for Bazel
load("//dependencies/compilers:dependencies.bzl", "antlr_dependencies")
antlr_dependencies()

# Load ANTLR dependencies for ANTLR programs
load("@rules_antlr//antlr:deps.bzl", "antlr_dependencies")
antlr_dependencies()


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

# Load GRPC Python dependencies
load("@stackb_rules_proto//python:deps.bzl", "python_grpc_compile")
python_grpc_compile()

# Load GRPC Node.js dependencies
load("@stackb_rules_proto//node:deps.bzl", "node_grpc_compile")
node_grpc_compile()


########################################
#     Load Deployment Dependencies     #
########################################

git_repository(
    name="graknlabs_rules_deployment",
    remote="https://github.com/graknlabs/deployment",
    commit="acc2b7baf4ab93a02dd49123d3adab4dc6989ce8",
)

load("@graknlabs_rules_deployment//github:dependencies.bzl", "dependencies_for_github_deployment")
dependencies_for_github_deployment()

load("@graknlabs_rules_deployment//maven:dependencies.bzl", maven_dependencies_for_deployment = "maven_dependencies")
maven_dependencies_for_deployment()
