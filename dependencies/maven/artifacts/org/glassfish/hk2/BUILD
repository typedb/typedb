java_library(
    name = "hk2-api",
    exports = [
        "//external:jar/org/glassfish/hk2/hk2-api"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/javax/inject:javax-inject",
        "//dependencies/maven/artifacts/org/glassfish/hk2/external:aopalliance-repackaged",
        ":hk2-utils"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hk2-locator",
    exports = [
        "//external:jar/org/glassfish/hk2/hk2-locator"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/glassfish/hk2/external:aopalliance-repackaged",
        "//dependencies/maven/artifacts/org/glassfish/hk2/external:javax-inject",
        "//dependencies/maven/artifacts/org/javassist:javassist",
        ":hk2-api",
        ":hk2-utils"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "hk2-utils",
    exports = [
        "//external:jar/org/glassfish/hk2/hk2-utils"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/javax/inject:javax-inject"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "osgi-resource-locator",
    exports = [
        "//external:jar/org/glassfish/hk2/osgi-resource-locator"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


