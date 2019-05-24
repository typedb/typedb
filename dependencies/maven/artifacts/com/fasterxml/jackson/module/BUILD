java_library(
    name = "jackson-module-paranamer",
    exports = [
        "//external:jar/com/fasterxml/jackson/module/jackson-module-paranamer"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-databind",
        "//dependencies/maven/artifacts/com/thoughtworks/paranamer:paranamer"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jackson-module-scala-2-11",
    exports = [
        "//external:jar/com/fasterxml/jackson/module/jackson-module-scala-2-11"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-annotations",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-core",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-databind",
        "//dependencies/maven/artifacts/org/scala-lang:scala-library",
        ":jackson-module-paranamer"
    ],
    visibility = [
        "//visibility:public"
    ]
)


