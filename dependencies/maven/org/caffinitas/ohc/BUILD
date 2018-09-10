java_library(
    name = "ohc_core",
    exports = [
        "//external:jar/org/caffinitas/ohc/ohc_core"
    ],
    runtime_deps = [
        "//dependencies/maven/com/google/guava:guava",
        "//dependencies/maven/net/java/dev/jna:jna",
        "//dependencies/maven/org/slf4j:slf4j_api"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "ohc_core_j8",
    exports = [
        "//external:jar/org/caffinitas/ohc/ohc_core_j8"
    ],
    runtime_deps = [
        ":ohc_core"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


