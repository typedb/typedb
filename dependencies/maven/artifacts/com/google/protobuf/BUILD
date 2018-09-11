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
        "//dependencies/maven/artifacts/com/google/code/gson:gson",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        ":protobuf_java"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


