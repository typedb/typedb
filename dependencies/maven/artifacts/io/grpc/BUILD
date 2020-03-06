java_library(
    name = "grpc-api",
    exports = [
        "//external:jar/io/grpc/grpc-api"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/errorprone:error-prone-annotations",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/codehaus/mojo:animal-sniffer-annotations",
        ":grpc-context"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc-context",
    exports = [
        "//external:jar/io/grpc/grpc-context"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "grpc-core",
    exports = [
        "//external:jar/io/grpc/grpc-core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/android:annotations",
        "//dependencies/maven/artifacts/com/google/code/gson:gson",
        "//dependencies/maven/artifacts/io/opencensus:opencensus-api",
        "//dependencies/maven/artifacts/io/opencensus:opencensus-contrib-grpc-metrics",
        "//dependencies/maven/artifacts/io/perfmark:perfmark-api",
        ":grpc-api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc-netty",
    exports = [
        "//external:jar/io/grpc/grpc-netty"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/io/netty:netty-codec-http2",
        "//dependencies/maven/artifacts/io/netty:netty-handler-proxy",
        ":grpc-core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc-protobuf",
    exports = [
        "//external:jar/io/grpc/grpc-protobuf"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/api/grpc:proto-google-common-protos",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        ":grpc-api",
        ":grpc-protobuf-lite"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc-protobuf-lite",
    exports = [
        "//external:jar/io/grpc/grpc-protobuf-lite"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        ":grpc-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "grpc-stub",
    exports = [
        "//external:jar/io/grpc/grpc-stub"
    ],
    runtime_deps = [
        ":grpc-api"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "grpc-testing",
    exports = [
        "//external:jar/io/grpc/grpc-testing"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/junit:junit",
        ":grpc-core",
        ":grpc-stub"
    ],
    visibility = [
        "//visibility:public"
    ]
)


