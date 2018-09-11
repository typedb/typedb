java_library(
    name = "curator_client",
    exports = [
        "//external:jar/org/apache/curator/curator_client"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/apache/zookeeper:zookeeper"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "curator_framework",
    exports = [
        "//external:jar/org/apache/curator/curator_framework"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/apache/zookeeper:zookeeper",
        ":curator_client"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "curator_recipes",
    exports = [
        "//external:jar/org/apache/curator/curator_recipes"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/apache/zookeeper:zookeeper",
        ":curator_framework"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


