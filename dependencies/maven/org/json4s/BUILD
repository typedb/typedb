java_library(
    name = "json4s_ast_2_10",
    exports = [
        "//external:jar/org/json4s/json4s_ast_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/org/scala_lang:scala_library"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "json4s_core_2_10",
    exports = [
        "//external:jar/org/json4s/json4s_core_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/com/thoughtworks/paranamer:paranamer",
        "//dependencies/maven/org/scala_lang:scala_library",
        "//dependencies/maven/org/scala_lang:scalap",
        ":json4s_ast_2_10"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "json4s_jackson_2_10",
    exports = [
        "//external:jar/org/json4s/json4s_jackson_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/com/fasterxml/jackson/core:jackson_databind",
        "//dependencies/maven/org/scala_lang:scala_library",
        ":json4s_core_2_10"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


