java_library(
    name = "brave",
    exports = [
        "//external:jar/io/zipkin/brave/brave"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/io/zipkin/reporter2:zipkin-reporter",
        "//dependencies/maven/artifacts/io/zipkin/zipkin2:zipkin"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "brave-instrumentation-grpc",
    exports = [
        "//external:jar/io/zipkin/brave/brave-instrumentation-grpc"
    ],
    runtime_deps = [
        ":brave"
    ],
    visibility = [
        "//visibility:public"
    ]
)


