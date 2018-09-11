java_library(
    name = "ST4",
    exports = [
        "//external:jar/org/antlr/ST4"
    ],
    runtime_deps = [
        ":antlr_runtime"
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
        ":antlr_runtime"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "antlr4_runtime",
    exports = [
        "//external:jar/org/antlr/antlr4_runtime"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/abego/treelayout:org_abego_treelayout_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "antlr_runtime",
    exports = [
        "//external:jar/org/antlr/antlr_runtime"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


