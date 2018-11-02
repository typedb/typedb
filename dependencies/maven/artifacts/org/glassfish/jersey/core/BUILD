java_library(
    name = "jersey-client",
    exports = [
        "//external:jar/org/glassfish/jersey/core/jersey-client"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/javax/ws/rs:javax-ws-rs-api",
        "//dependencies/maven/artifacts/org/glassfish/hk2/external:javax-inject",
        "//dependencies/maven/artifacts/org/glassfish/hk2:hk2-api",
        "//dependencies/maven/artifacts/org/glassfish/hk2:hk2-locator",
        ":jersey-common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jersey-common",
    exports = [
        "//external:jar/org/glassfish/jersey/core/jersey-common"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/javax/annotation:javax-annotation-api",
        "//dependencies/maven/artifacts/javax/ws/rs:javax-ws-rs-api",
        "//dependencies/maven/artifacts/org/glassfish/hk2/external:javax-inject",
        "//dependencies/maven/artifacts/org/glassfish/hk2:hk2-api",
        "//dependencies/maven/artifacts/org/glassfish/hk2:hk2-locator",
        "//dependencies/maven/artifacts/org/glassfish/hk2:osgi-resource-locator",
        "//dependencies/maven/artifacts/org/glassfish/jersey/bundles/repackaged:jersey-guava"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jersey-server",
    exports = [
        "//external:jar/org/glassfish/jersey/core/jersey-server"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/javax/annotation:javax-annotation-api",
        "//dependencies/maven/artifacts/javax/validation:validation-api",
        "//dependencies/maven/artifacts/javax/ws/rs:javax-ws-rs-api",
        "//dependencies/maven/artifacts/org/glassfish/hk2/external:javax-inject",
        "//dependencies/maven/artifacts/org/glassfish/hk2:hk2-api",
        "//dependencies/maven/artifacts/org/glassfish/hk2:hk2-locator",
        "//dependencies/maven/artifacts/org/glassfish/jersey/media:jersey-media-jaxb",
        ":jersey-client",
        ":jersey-common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


