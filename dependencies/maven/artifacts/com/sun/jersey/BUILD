java_library(
    name = "jersey-client",
    exports = [
        "//external:jar/com/sun/jersey/jersey-client"
    ],
    runtime_deps = [
        ":jersey-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jersey-core",
    exports = [
        "//external:jar/com/sun/jersey/jersey-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jersey-json",
    exports = [
        "//external:jar/com/sun/jersey/jersey-json"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/sun/xml/bind:jaxb-impl",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-core-asl",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-jaxrs",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-mapper-asl",
        "//dependencies/maven/artifacts/org/codehaus/jackson:jackson-xc",
        "//dependencies/maven/artifacts/org/codehaus/jettison:jettison",
        ":jersey-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jersey-server",
    exports = [
        "//external:jar/com/sun/jersey/jersey-server"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/asm:asm",
        ":jersey-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


