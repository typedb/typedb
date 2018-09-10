java_library(
    name = "curator_client",
    exports = [
        "//external:jar/org/apache/curator/curator_client"
    ],
    runtime_deps = [
        "//dependencies/maven/com/google/guava:guava",
        "//dependencies/maven/org/apache/zookeeper:zookeeper"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "curator_framework",
    exports = [
        "//external:jar/org/apache/curator/curator_framework"
    ],
    runtime_deps = [
        "//dependencies/maven/com/google/guava:guava",
        "//dependencies/maven/org/apache/zookeeper:zookeeper",
        ":curator_client"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "curator_recipes",
    exports = [
        "//external:jar/org/apache/curator/curator_recipes"
    ],
    runtime_deps = [
        "//dependencies/maven/com/google/guava:guava",
        "//dependencies/maven/org/apache/zookeeper:zookeeper",
        ":curator_framework"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


