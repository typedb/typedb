java_library(
    name = "httpasyncclient",
    exports = [
        "//external:jar/org/apache/httpcomponents/httpasyncclient"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "httpclient",
    exports = [
        "//external:jar/org/apache/httpcomponents/httpclient"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/commons_codec:commons_codec",
        "//dependencies/maven/artifacts/commons_logging:commons_logging",
        ":httpcore"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "httpcore",
    exports = [
        "//external:jar/org/apache/httpcomponents/httpcore"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "httpcore_nio",
    exports = [
        "//external:jar/org/apache/httpcomponents/httpcore_nio"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "httpmime",
    exports = [
        "//external:jar/org/apache/httpcomponents/httpmime"
    ],
    runtime_deps = [
        ":httpclient"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


