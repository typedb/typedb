# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//distribution/artifact:rules.bzl", "native_artifact_files")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")

def vaticle_typedb_console_artifact():
    native_artifact_files(
        name = "vaticle_typedb_console_artifact",
        group_name = "typedb-console-{platform}",
        artifact_name = "typedb-console-{platform}-{version}.{ext}",
        tag_source = deployment["artifact"]["release"]["download"],
        commit_source = deployment["artifact"]["snapshot"]["download"],
        tag = "2.28.4",
    )
