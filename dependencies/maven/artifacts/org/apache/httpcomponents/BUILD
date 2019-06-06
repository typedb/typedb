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
        "//dependencies/maven/artifacts/commons-codec:commons-codec",
        "//dependencies/maven/artifacts/commons-logging:commons-logging",
        ":httpcore"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "httpcore",
    exports = [
        "//external:jar/org/apache/httpcomponents/httpcore"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "httpcore-nio",
    exports = [
        "//external:jar/org/apache/httpcomponents/httpcore-nio"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


