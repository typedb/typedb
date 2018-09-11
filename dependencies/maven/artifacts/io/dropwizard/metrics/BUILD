java_library(
    name = "metrics_core",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "metrics_graphite",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics_graphite"
    ],
    runtime_deps = [
        ":metrics_core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "metrics_healthchecks",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics_healthchecks"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "metrics_json",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics_json"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson_databind",
        ":metrics_core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "metrics_jvm",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics_jvm"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/slf4j:slf4j_api",
        ":metrics_core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "metrics_servlets",
    exports = [
        "//external:jar/io/dropwizard/metrics/metrics_servlets"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/papertrail:profiler",
        ":metrics_core",
        ":metrics_healthchecks",
        ":metrics_json",
        ":metrics_jvm"
    ],
    visibility = [
        "//visibility:public"
    ]
)


