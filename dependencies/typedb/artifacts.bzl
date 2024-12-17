# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//distribution/artifact:rules.bzl", "native_artifact_files")
load("@typedb_dependencies//distribution:deployment.bzl", "deployment")

def typedb_console_artifact():
    native_artifact_files(
        name = "typedb_console_artifact",
        group_name = "typedb-console-{platform}",
        artifact_name = "typedb-console-{platform}-{version}.{ext}",
        tag_source = deployment["artifact"]["release"]["download"],
        commit_source = deployment["artifact"]["snapshot"]["download"],
        commit = "215db3feb9b8e5dde3f15534b44679bdf5004532"
    )
