java_library(
    name = "reporter_config3",
    exports = [
        "//external:jar/com/addthis/metrics/reporter_config3"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics_core",
        "//dependencies/maven/artifacts/org/apache/commons:commons_lang3",
        "//dependencies/maven/artifacts/org/hibernate:hibernate_validator",
        "//dependencies/maven/artifacts/org/slf4j:slf4j_api",
        "//dependencies/maven/artifacts/org/yaml:snakeyaml",
        ":reporter_config_base"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "reporter_config_base",
    exports = [
        "//external:jar/com/addthis/metrics/reporter_config_base"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apache/commons:commons_lang3",
        "//dependencies/maven/artifacts/org/hibernate:hibernate_validator",
        "//dependencies/maven/artifacts/org/slf4j:slf4j_api",
        "//dependencies/maven/artifacts/org/yaml:snakeyaml"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


