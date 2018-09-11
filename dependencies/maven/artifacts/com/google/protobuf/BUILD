java_library(
    name = "protobuf-java",
    exports = [
        "//external:jar/com/google/protobuf/protobuf-java"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "protobuf-java-util",
    exports = [
        "//external:jar/com/google/protobuf/protobuf-java-util"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/code/gson:gson",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        ":protobuf-java"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


