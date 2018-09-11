java_library(
    name = "tachyon_client",
    exports = [
        "//external:jar/org/tachyonproject/tachyon_client"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons_io:commons_io",
        "//dependencies/maven/artifacts/commons_lang:commons_lang",
        "//dependencies/maven/artifacts/org/apache/commons:commons_lang3",
        ":tachyon_underfs_hdfs",
        ":tachyon_underfs_local",
        ":tachyon_underfs_s3"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "tachyon_underfs_hdfs",
    exports = [
        "//external:jar/org/tachyonproject/tachyon_underfs_hdfs"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/apache/commons:commons_lang3"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "tachyon_underfs_local",
    exports = [
        "//external:jar/org/tachyonproject/tachyon_underfs_local"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "tachyon_underfs_s3",
    exports = [
        "//external:jar/org/tachyonproject/tachyon_underfs_s3"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


