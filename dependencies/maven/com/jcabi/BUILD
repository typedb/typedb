java_library(
    name = "jcabi_aspects",
    exports = [
        "//external:jar/com/jcabi/jcabi_aspects"
    ],
    runtime_deps = [
        "//dependencies/maven/javax/validation:validation_api",
        "//dependencies/maven/org/aspectj:aspectjrt"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "jcabi_log",
    exports = [
        "//external:jar/com/jcabi/jcabi_log"
    ],
    runtime_deps = [
        ":jcabi_aspects"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "jcabi_manifests",
    exports = [
        "//external:jar/com/jcabi/jcabi_manifests"
    ],
    runtime_deps = [
        ":jcabi_log"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


