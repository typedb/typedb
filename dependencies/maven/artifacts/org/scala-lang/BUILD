java_library(
    name = "scala-compiler",
    exports = [
        "//external:jar/org/scala-lang/scala-compiler"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/scala-lang/modules:scala-parser-combinators-2-11",
        "//dependencies/maven/artifacts/org/scala-lang/modules:scala-xml-2-11",
        ":scala-library",
        ":scala-reflect"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "scala-library",
    exports = [
        "//external:jar/org/scala-lang/scala-library"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "scala-reflect",
    exports = [
        "//external:jar/org/scala-lang/scala-reflect"
    ],
    runtime_deps = [
        ":scala-library"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "scalap",
    exports = [
        "//external:jar/org/scala-lang/scalap"
    ],
    runtime_deps = [
        ":scala-compiler"
    ],
    visibility = [
        "//visibility:public"
    ]
)


