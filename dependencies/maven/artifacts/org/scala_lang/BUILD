java_library(
    name = "scala_compiler",
    exports = [
        "//external:jar/org/scala_lang/scala_compiler"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/scala_lang/modules:scala_parser_combinators_2_11",
        "//dependencies/maven/artifacts/org/scala_lang/modules:scala_xml_2_11",
        ":scala_library",
        ":scala_reflect"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "scala_library",
    exports = [
        "//external:jar/org/scala_lang/scala_library"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "scala_reflect",
    exports = [
        "//external:jar/org/scala_lang/scala_reflect"
    ],
    runtime_deps = [
        ":scala_library"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "scalap",
    exports = [
        "//external:jar/org/scala_lang/scalap"
    ],
    runtime_deps = [
        ":scala_compiler"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


