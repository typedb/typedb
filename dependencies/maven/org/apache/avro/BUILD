java_library(
    name = "avro",
    exports = [
        "//external:jar/org/apache/avro/avro"
    ],
    runtime_deps = [
        "//dependencies/maven/com/thoughtworks/paranamer:paranamer",
        "//dependencies/maven/org/apache/commons:commons_compress",
        "//dependencies/maven/org/codehaus/jackson:jackson_core_asl",
        "//dependencies/maven/org/codehaus/jackson:jackson_mapper_asl",
        "//dependencies/maven/org/xerial/snappy:snappy_java"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "avro_ipc",
    exports = [
        "//external:jar/org/apache/avro/avro_ipc"
    ],
    runtime_deps = [
        "//dependencies/maven/org/codehaus/jackson:jackson_core_asl",
        "//dependencies/maven/org/codehaus/jackson:jackson_mapper_asl",
        ":avro"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "avro_ipc_jar_tests",
    exports = [
        "//external:jar/org/apache/avro/avro_ipc_jar_tests"
    ],
    runtime_deps = [
        "//dependencies/maven/org/codehaus/jackson:jackson_core_asl",
        "//dependencies/maven/org/codehaus/jackson:jackson_mapper_asl",
        ":avro"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "avro_mapred_jar_hadoop2",
    exports = [
        "//external:jar/org/apache/avro/avro_mapred_jar_hadoop2"
    ],
    runtime_deps = [
        "//dependencies/maven/org/codehaus/jackson:jackson_core_asl",
        "//dependencies/maven/org/codehaus/jackson:jackson_mapper_asl",
        ":avro_ipc",
        ":avro_ipc_jar_tests"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


