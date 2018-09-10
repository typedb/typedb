java_library(
    name = "chill_2_10",
    exports = [
        "//external:jar/com/twitter/chill_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/com/esotericsoftware/kryo:kryo",
        "//dependencies/maven/org/scala_lang:scala_library",
        ":chill_java"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "chill_java",
    exports = [
        "//external:jar/com/twitter/chill_java"
    ],
    runtime_deps = [
        "//dependencies/maven/com/esotericsoftware:kryo_shaded"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


