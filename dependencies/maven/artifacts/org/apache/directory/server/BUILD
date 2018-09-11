java_library(
    name = "apacheds_i18n",
    exports = [
        "//external:jar/org/apache/directory/server/apacheds_i18n"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "apacheds_kerberos_codec",
    exports = [
        "//external:jar/org/apache/directory/server/apacheds_kerberos_codec"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apache/directory/api:api_asn1_api",
        "//dependencies/maven/artifacts/org/apache/directory/api:api_util",
        ":apacheds_i18n"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


