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

load("@vaticle_dependencies//builder/java:rules.bzl", "native_dep_for_host_platform")
load("@io_bazel_rules_kotlin//kotlin:kotlin.bzl", "kt_jvm_test")

def typedb_java_test(name, server_artifacts, console_artifacts = {},
                      native_libraries_deps = [], deps = [], classpath_resources = [], data = [], args = [], **kwargs):

    native_server_artifact_paths, native_server_artifact_labels = native_artifact_paths_and_labels(server_artifacts)
    native_console_artifact_paths, native_console_artifact_labels = native_artifact_paths_and_labels(console_artifacts, mandatory = False)
    native_dependencies = get_native_dependencies(native_libraries_deps)

    native.java_test(
        name = name,
        deps = depset(deps + ["//test/runner:typedb-runner"]).to_list() + native_dependencies,
        classpath_resources = depset(classpath_resources + ["//test/runner:logback"]).to_list(),
        data = data + select(native_server_artifact_labels) + (select(native_console_artifact_labels) if native_console_artifact_labels else []),
        args = ["--server"] + select(native_server_artifact_paths) + ((["--console"] + select(native_console_artifact_paths)) if native_console_artifact_paths else []) + args,
        **kwargs
    )

def typedb_kt_test(name, server_artifacts, console_artifacts = {},
                        native_libraries_deps = [], deps = [], data = [], args = [], **kwargs):
    native_server_artifact_paths, native_server_artifact_labels = native_artifact_paths_and_labels(server_artifacts)
    native_console_artifact_paths, native_console_artifact_labels = native_artifact_paths_and_labels(console_artifacts, mandatory = False)
    native_dependencies = get_native_dependencies(native_libraries_deps)

    kt_jvm_test(
        name = name,
        deps = depset(deps + ["//test/runner:typedb-runner"]).to_list() + native_dependencies,
        data = data + select(native_server_artifact_labels) + (select(native_console_artifact_labels) if native_console_artifact_labels else []),
        args = ["--server"] + select(native_server_artifact_paths) + ((["--console"] + select(native_console_artifact_paths)) if native_console_artifact_paths else []) + args,
        **kwargs
    )

def get_native_dependencies(native_libraries_deps):
    native_dependencies = []
    for dep in native_libraries_deps:
       native_dependencies = native_dependencies + native_dep_for_host_platform(dep)
    return native_dependencies


def native_artifact_paths_and_labels(native_artifacts, mandatory = True):
    if native_artifacts:
        native_artifact_paths = {}
        native_artifact_labels = {}
        for key in native_artifacts.keys():
            native_artifact_labels[key] = [ native_artifacts[key] ]
            native_artifact_paths[key] = [ "$(location {})".format(native_artifacts[key]) ]
        return native_artifact_paths, native_artifact_labels
    elif mandatory:
        fail("Mandatory artifacts weren't available.")
    else:
        return [], []


def native_typedb_artifact(name, native_artifacts, output, **kwargs):
    native.genrule(
        name = name,
        outs = [output],
        srcs = select(native_artifacts, no_match_error = "There is no TypeDB artifact compatible with this operating system."),
        cmd = "read -a srcs <<< '$(SRCS)' && read -a outs <<< '$(OUTS)' && cp $${srcs[0]} $${outs[0]} && echo $${outs[0]}",
        **kwargs
    )
