java_library(
    name = "cucumber-core",
    exports = [
        "//external:jar/io/cucumber/cucumber-core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apiguardian:apiguardian-api",
        ":cucumber-expressions",
        ":cucumber-gherkin",
        ":cucumber-gherkin-vintage",
        ":cucumber-plugin",
        ":datatable",
        ":docstring",
        ":tag-expressions"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "cucumber-expressions",
    exports = [
        "//external:jar/io/cucumber/cucumber-expressions"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apiguardian:apiguardian-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "cucumber-gherkin",
    exports = [
        "//external:jar/io/cucumber/cucumber-gherkin"
    ],
    runtime_deps = [
        ":cucumber-plugin"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "cucumber-gherkin-vintage",
    exports = [
        "//external:jar/io/cucumber/cucumber-gherkin-vintage"
    ],
    runtime_deps = [
        ":cucumber-gherkin"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "cucumber-java",
    exports = [
        "//external:jar/io/cucumber/cucumber-java"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apiguardian:apiguardian-api",
        ":cucumber-core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "cucumber-junit",
    exports = [
        "//external:jar/io/cucumber/cucumber-junit"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/junit:junit",
        "//dependencies/maven/artifacts/org/apiguardian:apiguardian-api",
        ":cucumber-core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "cucumber-plugin",
    exports = [
        "//external:jar/io/cucumber/cucumber-plugin"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apiguardian:apiguardian-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "datatable",
    exports = [
        "//external:jar/io/cucumber/datatable"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apiguardian:apiguardian-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "docstring",
    exports = [
        "//external:jar/io/cucumber/docstring"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apiguardian:apiguardian-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "tag-expressions",
    exports = [
        "//external:jar/io/cucumber/tag-expressions"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


