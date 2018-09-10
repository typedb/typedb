java_library(
    name = "sesame_model",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame_model"
    ],
    runtime_deps = [
        ":sesame_util"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "sesame_rio_api",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame_rio_api"
    ],
    runtime_deps = [
        ":sesame_model",
        ":sesame_util"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "sesame_rio_datatypes",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame_rio_datatypes"
    ],
    runtime_deps = [
        ":sesame_model",
        ":sesame_rio_api",
        ":sesame_util"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "sesame_rio_languages",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame_rio_languages"
    ],
    runtime_deps = [
        ":sesame_model",
        ":sesame_rio_api",
        ":sesame_util"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "sesame_rio_n3",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame_rio_n3"
    ],
    runtime_deps = [
        ":sesame_model",
        ":sesame_rio_api",
        ":sesame_rio_datatypes",
        ":sesame_rio_languages",
        ":sesame_rio_turtle"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "sesame_rio_ntriples",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame_rio_ntriples"
    ],
    runtime_deps = [
        "//dependencies/maven/commons_io:commons_io",
        ":sesame_model",
        ":sesame_rio_api",
        ":sesame_rio_datatypes",
        ":sesame_rio_languages"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "sesame_rio_rdfxml",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame_rio_rdfxml"
    ],
    runtime_deps = [
        "//dependencies/maven/commons_io:commons_io",
        ":sesame_model",
        ":sesame_rio_api",
        ":sesame_rio_datatypes",
        ":sesame_rio_languages",
        ":sesame_util"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "sesame_rio_trig",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame_rio_trig"
    ],
    runtime_deps = [
        ":sesame_model",
        ":sesame_rio_api",
        ":sesame_rio_datatypes",
        ":sesame_rio_languages",
        ":sesame_rio_turtle",
        ":sesame_util"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "sesame_rio_trix",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame_rio_trix"
    ],
    runtime_deps = [
        "//dependencies/maven/commons_io:commons_io",
        ":sesame_model",
        ":sesame_rio_api",
        ":sesame_rio_datatypes",
        ":sesame_rio_languages",
        ":sesame_util"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "sesame_rio_turtle",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame_rio_turtle"
    ],
    runtime_deps = [
        "//dependencies/maven/commons_io:commons_io",
        ":sesame_model",
        ":sesame_rio_api",
        ":sesame_rio_datatypes",
        ":sesame_rio_languages",
        ":sesame_util"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "sesame_util",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame_util"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


