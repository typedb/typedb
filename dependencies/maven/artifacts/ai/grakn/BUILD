java_library(
    name = "benchmark-lib",
    exports = [
        "//external:jar/ai/grakn/benchmark-lib"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/io/zipkin/brave:brave",
        "//dependencies/maven/artifacts/io/zipkin/reporter2:zipkin-reporter",
        "//dependencies/maven/artifacts/io/zipkin/reporter2:zipkin-sender-urlconnection",
        "//dependencies/maven/artifacts/io/zipkin/zipkin2:zipkin",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        ":rpc"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "rpc",
    exports = [
        "//external:jar/ai/grakn/rpc"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-protobuf",
        "//dependencies/maven/artifacts/io/grpc:grpc-stub"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


