java_library(
    name = "netty-all",
    exports = [
        "//external:jar/io/netty/netty-all"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty-buffer",
    exports = [
        "//external:jar/io/netty/netty-buffer"
    ],
    runtime_deps = [
        ":netty-common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "netty-codec",
    exports = [
        "//external:jar/io/netty/netty-codec"
    ],
    runtime_deps = [
        ":netty-buffer",
        ":netty-common",
        ":netty-transport"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "netty-codec-http",
    exports = [
        "//external:jar/io/netty/netty-codec-http"
    ],
    runtime_deps = [
        ":netty-buffer",
        ":netty-codec",
        ":netty-common",
        ":netty-handler",
        ":netty-transport"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "netty-codec-http2",
    exports = [
        "//external:jar/io/netty/netty-codec-http2"
    ],
    runtime_deps = [
        ":netty-buffer",
        ":netty-codec",
        ":netty-codec-http",
        ":netty-common",
        ":netty-handler",
        ":netty-transport"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "netty-codec-socks",
    exports = [
        "//external:jar/io/netty/netty-codec-socks"
    ],
    runtime_deps = [
        ":netty-buffer",
        ":netty-codec",
        ":netty-common",
        ":netty-transport"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "netty-common",
    exports = [
        "//external:jar/io/netty/netty-common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "netty-handler",
    exports = [
        "//external:jar/io/netty/netty-handler"
    ],
    runtime_deps = [
        ":netty-buffer",
        ":netty-codec",
        ":netty-common",
        ":netty-transport"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "netty-handler-proxy",
    exports = [
        "//external:jar/io/netty/netty-handler-proxy"
    ],
    runtime_deps = [
        ":netty-buffer",
        ":netty-codec",
        ":netty-codec-http",
        ":netty-codec-socks",
        ":netty-common",
        ":netty-transport"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "netty-resolver",
    exports = [
        "//external:jar/io/netty/netty-resolver"
    ],
    runtime_deps = [
        ":netty-common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "netty-transport",
    exports = [
        "//external:jar/io/netty/netty-transport"
    ],
    runtime_deps = [
        ":netty-buffer",
        ":netty-common",
        ":netty-resolver"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


