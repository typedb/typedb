java_library(
    name = "zipkin-reporter",
    exports = [
        "//external:jar/io/zipkin/reporter2/zipkin-reporter"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/io/zipkin/zipkin2:zipkin"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "zipkin-sender-urlconnection",
    exports = [
        "//external:jar/io/zipkin/reporter2/zipkin-sender-urlconnection"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/io/zipkin/zipkin2:zipkin",
        ":zipkin-reporter"
    ],
    visibility = [
        "//visibility:public"
    ]
)


