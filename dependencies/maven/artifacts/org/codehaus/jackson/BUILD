java_library(
    name = "jackson-core-asl",
    exports = [
        "//external:jar/org/codehaus/jackson/jackson-core-asl"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jackson-jaxrs",
    exports = [
        "//external:jar/org/codehaus/jackson/jackson-jaxrs"
    ],
    runtime_deps = [
        ":jackson-core-asl",
        ":jackson-mapper-asl"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jackson-mapper-asl",
    exports = [
        "//external:jar/org/codehaus/jackson/jackson-mapper-asl"
    ],
    runtime_deps = [
        ":jackson-core-asl"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jackson-xc",
    exports = [
        "//external:jar/org/codehaus/jackson/jackson-xc"
    ],
    runtime_deps = [
        ":jackson-core-asl",
        ":jackson-mapper-asl"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


