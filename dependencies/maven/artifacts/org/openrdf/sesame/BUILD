java_library(
    name = "sesame-model",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame-model"
    ],
    runtime_deps = [
        ":sesame-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "sesame-rio-api",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame-rio-api"
    ],
    runtime_deps = [
        ":sesame-model",
        ":sesame-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "sesame-rio-datatypes",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame-rio-datatypes"
    ],
    runtime_deps = [
        ":sesame-model",
        ":sesame-rio-api",
        ":sesame-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "sesame-rio-languages",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame-rio-languages"
    ],
    runtime_deps = [
        ":sesame-model",
        ":sesame-rio-api",
        ":sesame-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "sesame-rio-n3",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame-rio-n3"
    ],
    runtime_deps = [
        ":sesame-model",
        ":sesame-rio-api",
        ":sesame-rio-datatypes",
        ":sesame-rio-languages",
        ":sesame-rio-turtle"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "sesame-rio-ntriples",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame-rio-ntriples"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/commons-io:commons-io",
        ":sesame-model",
        ":sesame-rio-api",
        ":sesame-rio-datatypes",
        ":sesame-rio-languages"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "sesame-rio-rdfxml",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame-rio-rdfxml"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/commons-io:commons-io",
        ":sesame-model",
        ":sesame-rio-api",
        ":sesame-rio-datatypes",
        ":sesame-rio-languages",
        ":sesame-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "sesame-rio-trig",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame-rio-trig"
    ],
    runtime_deps = [
        ":sesame-model",
        ":sesame-rio-api",
        ":sesame-rio-datatypes",
        ":sesame-rio-languages",
        ":sesame-rio-turtle",
        ":sesame-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "sesame-rio-trix",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame-rio-trix"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/commons-io:commons-io",
        ":sesame-model",
        ":sesame-rio-api",
        ":sesame-rio-datatypes",
        ":sesame-rio-languages",
        ":sesame-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "sesame-rio-turtle",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame-rio-turtle"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/commons-io:commons-io",
        ":sesame-model",
        ":sesame-rio-api",
        ":sesame-rio-datatypes",
        ":sesame-rio-languages",
        ":sesame-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "sesame-util",
    exports = [
        "//external:jar/org/openrdf/sesame/sesame-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


