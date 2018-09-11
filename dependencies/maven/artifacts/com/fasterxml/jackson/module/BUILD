java_library(
    name = "jackson_module_paranamer",
    exports = [
        "//external:jar/com/fasterxml/jackson/module/jackson_module_paranamer"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson_databind",
        "//dependencies/maven/artifacts/com/thoughtworks/paranamer:paranamer"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jackson_module_scala_2_10",
    exports = [
        "//external:jar/com/fasterxml/jackson/module/jackson_module_scala_2_10"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson_annotations",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson_core",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson_databind",
        "//dependencies/maven/artifacts/org/scala_lang:scala_library",
        "//dependencies/maven/artifacts/org/scala_lang:scala_reflect",
        ":jackson_module_paranamer"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "jackson_module_scala_2_11",
    exports = [
        "//external:jar/com/fasterxml/jackson/module/jackson_module_scala_2_11"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson_annotations",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson_core",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson_databind",
        "//dependencies/maven/artifacts/org/scala_lang:scala_reflect",
        ":jackson_module_paranamer"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


