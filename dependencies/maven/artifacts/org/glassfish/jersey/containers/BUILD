java_library(
    name = "jersey-container-servlet",
    exports = [
        "//external:jar/org/glassfish/jersey/containers/jersey-container-servlet"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/javax/ws/rs:javax-ws-rs-api",
        "//dependencies/maven/artifacts/org/glassfish/jersey/core:jersey-common",
        "//dependencies/maven/artifacts/org/glassfish/jersey/core:jersey-server",
        ":jersey-container-servlet-core"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jersey-container-servlet-core",
    exports = [
        "//external:jar/org/glassfish/jersey/containers/jersey-container-servlet-core"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/javax/ws/rs:javax-ws-rs-api",
        "//dependencies/maven/artifacts/org/glassfish/hk2/external:javax-inject",
        "//dependencies/maven/artifacts/org/glassfish/jersey/core:jersey-common",
        "//dependencies/maven/artifacts/org/glassfish/jersey/core:jersey-server"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


