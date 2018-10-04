java_library(
    name = "apacheds-i18n",
    exports = [
        "//external:jar/org/apache/directory/server/apacheds-i18n"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "apacheds-kerberos-codec",
    exports = [
        "//external:jar/org/apache/directory/server/apacheds-kerberos-codec"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apache/directory/api:api-asn1-api",
        "//dependencies/maven/artifacts/org/apache/directory/api:api-util",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        ":apacheds-i18n"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


