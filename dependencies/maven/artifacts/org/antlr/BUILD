java_library(
    name = "ST4",
    exports = [
        "//external:jar/org/antlr/ST4"
    ],
    runtime_deps = [
        ":antlr-runtime"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "antlr",
    exports = [
        "//external:jar/org/antlr/antlr"
    ],
    runtime_deps = [
        ":ST4",
        ":antlr-runtime"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "antlr-runtime",
    exports = [
        "//external:jar/org/antlr/antlr-runtime"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


