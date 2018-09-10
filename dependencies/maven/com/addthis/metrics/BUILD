java_library(
    name = "reporter_config3",
    exports = [
        "//external:jar/com/addthis/metrics/reporter_config3"
    ],
    runtime_deps = [
        "//dependencies/maven/io/dropwizard/metrics:metrics_core",
        "//dependencies/maven/org/apache/commons:commons_lang3",
        "//dependencies/maven/org/hibernate:hibernate_validator",
        "//dependencies/maven/org/slf4j:slf4j_api",
        "//dependencies/maven/org/yaml:snakeyaml",
        ":reporter_config_base"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "reporter_config_base",
    exports = [
        "//external:jar/com/addthis/metrics/reporter_config_base"
    ],
    runtime_deps = [
        "//dependencies/maven/org/apache/commons:commons_lang3",
        "//dependencies/maven/org/hibernate:hibernate_validator",
        "//dependencies/maven/org/slf4j:slf4j_api",
        "//dependencies/maven/org/yaml:snakeyaml"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


