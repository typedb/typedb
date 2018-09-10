java_library(
    name = "websocket_api",
    exports = [
        "//external:jar/org/eclipse/jetty/websocket/websocket_api"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "websocket_client",
    exports = [
        "//external:jar/org/eclipse/jetty/websocket/websocket_client"
    ],
    runtime_deps = [
        "//dependencies/maven/org/eclipse/jetty:jetty_io",
        "//dependencies/maven/org/eclipse/jetty:jetty_util",
        ":websocket_common"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "websocket_common",
    exports = [
        "//external:jar/org/eclipse/jetty/websocket/websocket_common"
    ],
    runtime_deps = [
        "//dependencies/maven/org/eclipse/jetty:jetty_io",
        "//dependencies/maven/org/eclipse/jetty:jetty_util",
        ":websocket_api"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "websocket_server",
    exports = [
        "//external:jar/org/eclipse/jetty/websocket/websocket_server"
    ],
    runtime_deps = [
        "//dependencies/maven/org/eclipse/jetty:jetty_http",
        "//dependencies/maven/org/eclipse/jetty:jetty_servlet",
        ":websocket_client",
        ":websocket_common",
        ":websocket_servlet"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "websocket_servlet",
    exports = [
        "//external:jar/org/eclipse/jetty/websocket/websocket_servlet"
    ],
    runtime_deps = [
        "//dependencies/maven/javax/servlet:javax_servlet_api",
        ":websocket_api"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


