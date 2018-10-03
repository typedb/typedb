java_library(
    name = "opencensus-api",
    exports = [
        "//external:jar/io/opencensus/opencensus-api"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/errorprone:error-prone-annotations"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "opencensus-contrib-grpc-metrics",
    exports = [
        "//external:jar/io/opencensus/opencensus-contrib-grpc-metrics"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/errorprone:error-prone-annotations",
        ":opencensus-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


