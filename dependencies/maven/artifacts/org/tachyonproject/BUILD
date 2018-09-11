java_library(
    name = "tachyon-client",
    exports = [
        "//external:jar/org/tachyonproject/tachyon-client"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        ":tachyon-underfs-hdfs",
        ":tachyon-underfs-local",
        ":tachyon-underfs-s3"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "tachyon-underfs-hdfs",
    exports = [
        "//external:jar/org/tachyonproject/tachyon-underfs-hdfs"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "tachyon-underfs-local",
    exports = [
        "//external:jar/org/tachyonproject/tachyon-underfs-local"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "tachyon-underfs-s3",
    exports = [
        "//external:jar/org/tachyonproject/tachyon-underfs-s3"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


