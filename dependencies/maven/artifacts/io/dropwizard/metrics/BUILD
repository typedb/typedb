java_library(
    name = "metrics-core",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics-core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "metrics-graphite",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics-graphite"
    ],
    runtime_deps = [
        ":metrics-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "metrics-json",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics-json"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-databind",
        ":metrics-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "metrics-jvm",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics-jvm"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        ":metrics-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


