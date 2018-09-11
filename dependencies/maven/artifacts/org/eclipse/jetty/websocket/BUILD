java_library(
    name = "websocket-api",
    exports = [
        "//external:jar/org/eclipse/jetty/websocket/websocket-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "websocket-client",
    exports = [
        "//external:jar/org/eclipse/jetty/websocket/websocket-client"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/eclipse/jetty:jetty-io",
        "//dependencies/maven/artifacts/org/eclipse/jetty:jetty-util",
        ":websocket-common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "websocket-common",
    exports = [
        "//external:jar/org/eclipse/jetty/websocket/websocket-common"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/eclipse/jetty:jetty-io",
        "//dependencies/maven/artifacts/org/eclipse/jetty:jetty-util",
        ":websocket-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "websocket-server",
    exports = [
        "//external:jar/org/eclipse/jetty/websocket/websocket-server"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/eclipse/jetty:jetty-http",
        "//dependencies/maven/artifacts/org/eclipse/jetty:jetty-servlet",
        ":websocket-client",
        ":websocket-common",
        ":websocket-servlet"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "websocket-servlet",
    exports = [
        "//external:jar/org/eclipse/jetty/websocket/websocket-servlet"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/javax/servlet:javax-servlet-api",
        ":websocket-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


