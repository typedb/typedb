java_library(
    name = "hbase_annotations",
    exports = [
        "//external:jar/org/apache/hbase/hbase_annotations"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/log4j:log4j"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hbase_client",
    exports = [
        "//external:jar/org/apache/hbase/hbase_client"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf_java",
        "//dependencies/maven/artifacts/commons_codec:commons_codec",
        "//dependencies/maven/artifacts/commons_lang:commons_lang",
        "//dependencies/maven/artifacts/commons_logging:commons_logging",
        "//dependencies/maven/artifacts/log4j:log4j",
        ":hbase_annotations",
        ":hbase_common",
        ":hbase_protocol"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hbase_common",
    exports = [
        "//external:jar/org/apache/hbase/hbase_common"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf_java",
        "//dependencies/maven/artifacts/commons_codec:commons_codec",
        "//dependencies/maven/artifacts/commons_collections:commons_collections",
        "//dependencies/maven/artifacts/commons_lang:commons_lang",
        "//dependencies/maven/artifacts/commons_logging:commons_logging",
        "//dependencies/maven/artifacts/log4j:log4j",
        "//dependencies/maven/artifacts/org/mortbay/jetty:jetty_util",
        ":hbase_annotations"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hbase_protocol",
    exports = [
        "//external:jar/org/apache/hbase/hbase_protocol"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf_java",
        "//dependencies/maven/artifacts/commons_logging:commons_logging",
        "//dependencies/maven/artifacts/log4j:log4j",
        ":hbase_annotations"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


