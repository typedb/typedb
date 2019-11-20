java_library(
    name = "groovy-cli-picocli",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy-cli-picocli"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/info/picocli:picocli"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "groovy-console",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy-console"
    ],
    runtime_deps = [
        ":groovy-cli-picocli",
        ":groovy-swing",
        ":groovy-templates"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "groovy-groovysh-jar-indy",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy-groovysh-jar-indy"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/jline:jline",
        ":groovy-cli-picocli",
        ":groovy-console"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "groovy-jar-indy",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy-jar-indy"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "groovy-json-jar-indy",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy-json-jar-indy"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "groovy-jsr223-jar-indy",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy-jsr223-jar-indy"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "groovy-swing",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy-swing"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "groovy-templates",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy-templates"
    ],
    runtime_deps = [
        ":groovy-xml"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "groovy-xml",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy-xml"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


