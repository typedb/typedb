java_library(
    name = "jackson_core_asl",
    exports = [
        "//external:jar/org/codehaus/jackson/jackson_core_asl"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jackson_jaxrs",
    exports = [
        "//external:jar/org/codehaus/jackson/jackson_jaxrs"
    ],
    runtime_deps = [
        ":jackson_core_asl"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jackson_mapper_asl",
    exports = [
        "//external:jar/org/codehaus/jackson/jackson_mapper_asl"
    ],
    runtime_deps = [
        ":jackson_core_asl"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jackson_xc",
    exports = [
        "//external:jar/org/codehaus/jackson/jackson_xc"
    ],
    runtime_deps = [
        ":jackson_core_asl"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


