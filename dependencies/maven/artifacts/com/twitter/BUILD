java_library(
    name = "chill-2-10",
    exports = [
        "//external:jar/com/twitter/chill-2-10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/esotericsoftware/kryo:kryo",
        "//dependencies/maven/artifacts/org/scala-lang:scala-library",
        ":chill-java"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "chill-java",
    exports = [
        "//external:jar/com/twitter/chill-java"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/esotericsoftware:kryo-shaded"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


