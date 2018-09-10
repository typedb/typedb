java_library(
    name = "groovy",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "groovy_console",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy_console"
    ],
    runtime_deps = [
        ":groovy",
        ":groovy_swing",
        ":groovy_templates"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "groovy_groovysh_jar_indy",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy_groovysh_jar_indy"
    ],
    runtime_deps = [
        "//dependencies/maven/jline:jline",
        ":groovy",
        ":groovy_console"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "groovy_jar_indy",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy_jar_indy"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "groovy_json",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy_json"
    ],
    runtime_deps = [
        ":groovy"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "groovy_json_jar_indy",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy_json_jar_indy"
    ],
    runtime_deps = [
        ":groovy"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "groovy_jsr223_jar_indy",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy_jsr223_jar_indy"
    ],
    runtime_deps = [
        ":groovy"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "groovy_swing",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy_swing"
    ],
    runtime_deps = [
        ":groovy"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "groovy_templates",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy_templates"
    ],
    runtime_deps = [
        ":groovy",
        ":groovy_xml"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "groovy_xml",
    exports = [
        "//external:jar/org/codehaus/groovy/groovy_xml"
    ],
    runtime_deps = [
        ":groovy"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


