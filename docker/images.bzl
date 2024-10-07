# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@io_bazel_rules_docker//container:pull.bzl", "container_pull")

def base_images():
    # FROM amd64/ubuntu:22.04
    # RUN apt-get -y update && apt-get install -y openjdk-11-jre-headless && rm -rf /var/lib/apt/lists
    container_pull(
        name = "ubuntu-22.04-x86_64",
        architecture = "amd64",
        registry = "index.docker.io",
        repository = "vaticle/ubuntu",
        digest = "sha256:2c45c92258ee82c28b5e2ab32cabe21e24116aa2a248fb510a4c4d625073217b",
    )

    # FROM arm64v8/ubuntu:22.04
    # RUN apt-get -y update && apt-get install -y openjdk-11-jre-headless && rm -rf /var/lib/apt/lists
    container_pull(
        name = "ubuntu-22.04-arm64",
        architecture="arm64",
        registry = "index.docker.io",
        repository = "vaticle/ubuntu",
        digest = "sha256:927ed8f99876bceff89856447719273e1b434d32b112a6b5528054334c82fb6d",
    )
