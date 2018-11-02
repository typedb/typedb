java_library(
    name = "curator-client",
    exports = [
        "//external:jar/org/apache/curator/curator-client"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/apache/zookeeper:zookeeper",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "curator-framework",
    exports = [
        "//external:jar/org/apache/curator/curator-framework"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/apache/zookeeper:zookeeper",
        ":curator-client"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "curator-recipes",
    exports = [
        "//external:jar/org/apache/curator/curator-recipes"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/apache/zookeeper:zookeeper",
        ":curator-framework"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


