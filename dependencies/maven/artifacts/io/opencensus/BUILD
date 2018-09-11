java_library(
    name = "opencensus_api",
    exports = [
        "//external:jar/io/opencensus/opencensus_api"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/errorprone:error_prone_annotations",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "opencensus_contrib_grpc_metrics",
    exports = [
        "//external:jar/io/opencensus/opencensus_contrib_grpc_metrics"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/errorprone:error_prone_annotations",
        ":opencensus_api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


