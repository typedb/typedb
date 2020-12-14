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

load("@graknlabs_bazel_distribution//artifact:rules.bzl", "artifact_file")
load("@graknlabs_dependencies//distribution:deployment.bzl", "deployment")

def graknlabs_console_artifact():
    artifact_file(
        name = "graknlabs_console_artifact",
        group_name = "graknlabs_console",
        artifact_name = "console-artifact.tgz",
        tag_source = deployment["artifact.release"],
        commit_source = deployment["artifact.snapshot"],
        commit = "2a740e144b445f469aa2d650051f2bfa67bc3c0b"
    )
