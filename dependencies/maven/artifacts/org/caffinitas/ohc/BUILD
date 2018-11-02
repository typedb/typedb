java_library(
    name = "ohc-core",
    exports = [
        "//external:jar/org/caffinitas/ohc/ohc-core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/net/java/dev/jna:jna",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "ohc-core-j8",
    exports = [
        "//external:jar/org/caffinitas/ohc/ohc-core-j8"
    ],
    runtime_deps = [
        ":ohc-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


