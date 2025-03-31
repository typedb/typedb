# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@io_bazel_rules_docker//container:pull.bzl", "container_pull")

def base_images():
    container_pull(
        name = "typedb-ubuntu-x86_64",
        architecture = "amd64",
        registry = "index.docker.io",
        repository = "typedb/ubuntu",
        tag = "3.1.0",
    )
    container_pull(
        name = "typedb-ubuntu-arm64",
        architecture = "arm64",
        registry = "index.docker.io",
        repository = "typedb/ubuntu",
        tag = "3.1.0",
    )
