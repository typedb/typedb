java_library(
    name = "jackson-annotations",
    exports = [
        "//external:jar/com/fasterxml/jackson/core/jackson-annotations"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jackson-core",
    exports = [
        "//external:jar/com/fasterxml/jackson/core/jackson-core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "jackson-databind",
    exports = [
        "//external:jar/com/fasterxml/jackson/core/jackson-databind"
    ],
    runtime_deps = [
        ":jackson-annotations",
        ":jackson-core"
    ],
    visibility = [
        "//visibility:public"
    ]
)


