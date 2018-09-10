java_library(
    name = "protobuf_java",
    exports = [
        "//external:jar/com/google/protobuf/protobuf_java"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "protobuf_java_util",
    exports = [
        "//external:jar/com/google/protobuf/protobuf_java_util"
    ],
    runtime_deps = [
        "//dependencies/maven/com/google/code/gson:gson",
        "//dependencies/maven/com/google/guava:guava",
        ":protobuf_java"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


