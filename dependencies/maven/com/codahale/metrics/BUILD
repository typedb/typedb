java_library(
    name = "metrics_core",
    exports = [
        "//external:jar/com/codahale/metrics/metrics_core"
    ],
    runtime_deps = [
        "//dependencies/maven/org/slf4j:slf4j_api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "metrics_ganglia",
    exports = [
        "//external:jar/com/codahale/metrics/metrics_ganglia"
    ],
    runtime_deps = [
        "//dependencies/maven/info/ganglia/gmetric4j:gmetric4j",
        ":metrics_core"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "metrics_graphite",
    exports = [
        "//external:jar/com/codahale/metrics/metrics_graphite"
    ],
    runtime_deps = [
        ":metrics_core"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


