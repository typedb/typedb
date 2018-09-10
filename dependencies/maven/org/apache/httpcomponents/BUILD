java_library(
    name = "httpasyncclient",
    exports = [
        "//external:jar/org/apache/httpcomponents/httpasyncclient"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "httpclient",
    exports = [
        "//external:jar/org/apache/httpcomponents/httpclient"
    ],
    runtime_deps = [
        "//dependencies/maven/commons_codec:commons_codec",
        "//dependencies/maven/commons_logging:commons_logging",
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
        "//dependencies/maven:__subpackages__"
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
        "//dependencies/maven:__subpackages__"
    ]
)


