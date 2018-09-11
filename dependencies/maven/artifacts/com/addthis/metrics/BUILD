java_library(
    name = "reporter-config-base",
    exports = [
        "//external:jar/com/addthis/metrics/reporter-config-base"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/hibernate:hibernate-validator",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/org/yaml:snakeyaml"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "reporter-config3",
    exports = [
        "//external:jar/com/addthis/metrics/reporter-config3"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-core",
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/hibernate:hibernate-validator",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/org/yaml:snakeyaml",
        ":reporter-config-base"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


