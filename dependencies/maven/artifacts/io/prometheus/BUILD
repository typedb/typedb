java_library(
    name = "simpleclient",
    exports = [
        "//external:jar/io/prometheus/simpleclient"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "simpleclient-common",
    exports = [
        "//external:jar/io/prometheus/simpleclient-common"
    ],
    runtime_deps = [
        ":simpleclient"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "simpleclient-dropwizard",
    exports = [
        "//external:jar/io/prometheus/simpleclient-dropwizard"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-core",
        ":simpleclient"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "simpleclient-servlet",
    exports = [
        "//external:jar/io/prometheus/simpleclient-servlet"
    ],
    runtime_deps = [
        ":simpleclient",
        ":simpleclient-common"
    ],
    visibility = [
        "//visibility:public"
    ]
)


