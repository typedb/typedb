java_library(
    name = "grpc_context",
    exports = [
        "//external:jar/io/grpc/grpc_context"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "grpc_core",
    exports = [
        "//external:jar/io/grpc/grpc_core"
    ],
    runtime_deps = [
        "//dependencies/maven/com/google/code/findbugs:jsr305",
        "//dependencies/maven/com/google/errorprone:error_prone_annotations",
        "//dependencies/maven/com/google/guava:guava",
        "//dependencies/maven/com/google/instrumentation:instrumentation_api",
        "//dependencies/maven/io/opencensus:opencensus_api",
        "//dependencies/maven/io/opencensus:opencensus_contrib_grpc_metrics",
        ":grpc_context"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_netty",
    exports = [
        "//external:jar/io/grpc/grpc_netty"
    ],
    runtime_deps = [
        "//dependencies/maven/io/netty:netty_codec_http2",
        "//dependencies/maven/io/netty:netty_handler_proxy",
        ":grpc_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_protobuf",
    exports = [
        "//external:jar/io/grpc/grpc_protobuf"
    ],
    runtime_deps = [
        "//dependencies/maven/com/google/api/grpc:proto_google_common_protos",
        "//dependencies/maven/com/google/guava:guava",
        "//dependencies/maven/com/google/protobuf:protobuf_java",
        "//dependencies/maven/com/google/protobuf:protobuf_java_util",
        ":grpc_core",
        ":grpc_protobuf_lite"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_protobuf_lite",
    exports = [
        "//external:jar/io/grpc/grpc_protobuf_lite"
    ],
    runtime_deps = [
        "//dependencies/maven/com/google/guava:guava",
        ":grpc_core"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "grpc_stub",
    exports = [
        "//external:jar/io/grpc/grpc_stub"
    ],
    runtime_deps = [
        ":grpc_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc_testing",
    exports = [
        "//external:jar/io/grpc/grpc_testing"
    ],
    runtime_deps = [
        "//dependencies/maven/junit:junit",
        "//dependencies/maven/org/mockito:mockito_core",
        ":grpc_core",
        ":grpc_stub"
    ],
    visibility = [
        "//visibility:public"
    ]
)


