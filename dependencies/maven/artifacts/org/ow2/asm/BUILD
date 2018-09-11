java_library(
    name = "asm",
    exports = [
        "//external:jar/org/ow2/asm/asm"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "asm-analysis",
    exports = [
        "//external:jar/org/ow2/asm/asm-analysis"
    ],
    runtime_deps = [
        ":asm-tree"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "asm-commons",
    exports = [
        "//external:jar/org/ow2/asm/asm-commons"
    ],
    runtime_deps = [
        ":asm-tree"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "asm-tree",
    exports = [
        "//external:jar/org/ow2/asm/asm-tree"
    ],
    runtime_deps = [
        ":asm"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "asm-util",
    exports = [
        "//external:jar/org/ow2/asm/asm-util"
    ],
    runtime_deps = [
        ":asm-tree"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


