java_library(
    name = "netty",
    exports = [
        "//external:jar/io/netty/netty"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



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
        ":netty-codec"
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
        ":netty-codec-http",
        ":netty-handler"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty-codec-socks",
    exports = [
        "//external:jar/io/netty/netty-codec-socks"
    ],
    runtime_deps = [
        ":netty-codec"
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
        ":netty-transport"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty-handler-proxy",
    exports = [
        "//external:jar/io/netty/netty-handler-proxy"
    ],
    runtime_deps = [
        ":netty-codec-http",
        ":netty-codec-socks",
        ":netty-transport"
    ],
    visibility = [
        "//visibility:public"
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
    name = "netty-tcnative-boringssl-static",
    exports = [
        "//external:jar/io/netty/netty-tcnative-boringssl-static"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "netty-transport",
    exports = [
        "//external:jar/io/netty/netty-transport"
    ],
    runtime_deps = [
        ":netty-buffer",
        ":netty-resolver"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


