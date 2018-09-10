java_library(
    name = "jersey_client",
    exports = [
        "//external:jar/com/sun/jersey/jersey_client"
    ],
    runtime_deps = [
        ":jersey_core"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "jersey_core",
    exports = [
        "//external:jar/com/sun/jersey/jersey_core"
    ],
    runtime_deps = [
        "//dependencies/maven/javax/ws/rs:jsr311_api"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "jersey_json",
    exports = [
        "//external:jar/com/sun/jersey/jersey_json"
    ],
    runtime_deps = [
        "//dependencies/maven/com/sun/xml/bind:jaxb_impl",
        "//dependencies/maven/org/codehaus/jackson:jackson_core_asl",
        "//dependencies/maven/org/codehaus/jackson:jackson_jaxrs",
        "//dependencies/maven/org/codehaus/jackson:jackson_xc",
        "//dependencies/maven/org/codehaus/jettison:jettison",
        ":jersey_core"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "jersey_server",
    exports = [
        "//external:jar/com/sun/jersey/jersey_server"
    ],
    runtime_deps = [
        "//dependencies/maven/asm:asm",
        ":jersey_core"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


