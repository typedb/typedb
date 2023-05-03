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

load("@vaticle_dependencies//distribution/artifact:rules.bzl", "native_artifact_files")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")

def vaticle_typedb_console_artifact():
    native_artifact_files(
        name = "vaticle_typedb_console_artifact",
        group_name = "vaticle_typedb_console",
        artifact_name = "typedb-console-{platform}-{version}.{ext}",
        tag_source = deployment["artifact.release"],
        commit_source = deployment["artifact.snapshot"],
        commit = "4ab2f4b7840163be7dd358b39a3c06ff06018481",
    )
