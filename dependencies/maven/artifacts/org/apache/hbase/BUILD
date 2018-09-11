java_library(
    name = "hbase-annotations",
    exports = [
        "//external:jar/org/apache/hbase/hbase-annotations"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/log4j:log4j"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hbase-client",
    exports = [
        "//external:jar/org/apache/hbase/hbase-client"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/commons-codec:commons-codec",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/commons-logging:commons-logging",
        "//dependencies/maven/artifacts/log4j:log4j",
        ":hbase-annotations",
        ":hbase-common",
        ":hbase-protocol"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hbase-common",
    exports = [
        "//external:jar/org/apache/hbase/hbase-common"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/commons-codec:commons-codec",
        "//dependencies/maven/artifacts/commons-collections:commons-collections",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/commons-logging:commons-logging",
        "//dependencies/maven/artifacts/log4j:log4j",
        "//dependencies/maven/artifacts/org/mortbay/jetty:jetty-util",
        ":hbase-annotations"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hbase-protocol",
    exports = [
        "//external:jar/org/apache/hbase/hbase-protocol"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/commons-logging:commons-logging",
        "//dependencies/maven/artifacts/log4j:log4j",
        ":hbase-annotations"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


