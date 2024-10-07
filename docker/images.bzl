# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@io_bazel_rules_docker//container:pull.bzl", "container_pull")

def base_images():
    container_pull(
        name = "ubuntu-22.04-x86_64",
        architecture = "amd64",
        registry = "index.docker.io",
        repository = "amd64/ubuntu",
        digest = "sha256:3d1556a8a18cf5307b121e0a98e93f1ddf1f3f8e092f1fddfd941254785b95d7",
    )
    container_pull(
        name = "ubuntu-22.04-arm64",
        architecture="arm64",
        registry = "index.docker.io",
        repository = "arm64v8/ubuntu",
        digest = "sha256:7c75ab2b0567edbb9d4834a2c51e462ebd709740d1f2c40bcd23c56e974fe2a8",
    )
