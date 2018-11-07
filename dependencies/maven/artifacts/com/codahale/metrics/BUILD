java_library(
    name = "metrics-core",
    exports = [
        "//external:jar/com/codahale/metrics/metrics-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "metrics-ganglia",
    exports = [
        "//external:jar/com/codahale/metrics/metrics-ganglia"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/info/ganglia/gmetric4j:gmetric4j",
        ":metrics-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "metrics-graphite",
    exports = [
        "//external:jar/com/codahale/metrics/metrics-graphite"
    ],
    runtime_deps = [
        ":metrics-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


