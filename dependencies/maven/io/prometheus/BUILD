java_library(
    name = "simpleclient",
    exports = [
        "//external:jar/io/prometheus/simpleclient"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "simpleclient_common",
    exports = [
        "//external:jar/io/prometheus/simpleclient_common"
    ],
    runtime_deps = [
        ":simpleclient"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "simpleclient_dropwizard",
    exports = [
        "//external:jar/io/prometheus/simpleclient_dropwizard"
    ],
    runtime_deps = [
        "//dependencies/maven/io/dropwizard/metrics:metrics_core",
        ":simpleclient"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "simpleclient_servlet",
    exports = [
        "//external:jar/io/prometheus/simpleclient_servlet"
    ],
    runtime_deps = [
        ":simpleclient",
        ":simpleclient_common"
    ],
    visibility = [
        "//visibility:public"
    ]
)


