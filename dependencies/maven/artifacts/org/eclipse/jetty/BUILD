java_library(
    name = "jetty-http",
    exports = [
        "//external:jar/org/eclipse/jetty/jetty-http"
    ],
    runtime_deps = [
        ":jetty-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jetty-io",
    exports = [
        "//external:jar/org/eclipse/jetty/jetty-io"
    ],
    runtime_deps = [
        ":jetty-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jetty-security",
    exports = [
        "//external:jar/org/eclipse/jetty/jetty-security"
    ],
    runtime_deps = [
        ":jetty-server"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jetty-server",
    exports = [
        "//external:jar/org/eclipse/jetty/jetty-server"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/javax/servlet:javax-servlet-api",
        ":jetty-http",
        ":jetty-io"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jetty-servlet",
    exports = [
        "//external:jar/org/eclipse/jetty/jetty-servlet"
    ],
    runtime_deps = [
        ":jetty-security"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jetty-util",
    exports = [
        "//external:jar/org/eclipse/jetty/jetty-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jetty-webapp",
    exports = [
        "//external:jar/org/eclipse/jetty/jetty-webapp"
    ],
    runtime_deps = [
        ":jetty-servlet",
        ":jetty-xml"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jetty-xml",
    exports = [
        "//external:jar/org/eclipse/jetty/jetty-xml"
    ],
    runtime_deps = [
        ":jetty-util"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


