java_library(
    name = "json4s-ast-2-10",
    exports = [
        "//external:jar/org/json4s/json4s-ast-2-10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/scala-lang:scala-library"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "json4s-core-2-10",
    exports = [
        "//external:jar/org/json4s/json4s-core-2-10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/thoughtworks/paranamer:paranamer",
        "//dependencies/maven/artifacts/org/scala-lang:scala-library",
        "//dependencies/maven/artifacts/org/scala-lang:scalap",
        ":json4s-ast-2-10"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "json4s-jackson-2-10",
    exports = [
        "//external:jar/org/json4s/json4s-jackson-2-10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-databind",
        "//dependencies/maven/artifacts/org/scala-lang:scala-library",
        ":json4s-core-2-10"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


