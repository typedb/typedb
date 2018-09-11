java_library(
    name = "json_path",
    exports = [
        "//external:jar/com/jayway/restassured/json_path"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy_json",
        ":rest_assured_common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "rest_assured",
    exports = [
        "//external:jar/com/jayway/restassured/rest_assured"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apache/httpcomponents:httpclient",
        "//dependencies/maven/artifacts/org/apache/httpcomponents:httpmime",
        "//dependencies/maven/artifacts/org/ccil/cowan/tagsoup:tagsoup",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy_xml",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest_core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest_library",
        ":json_path",
        ":xml_path"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "rest_assured_common",
    exports = [
        "//external:jar/com/jayway/restassured/rest_assured_common"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apache/commons:commons_lang3",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "xml_path",
    exports = [
        "//external:jar/com/jayway/restassured/xml_path"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apache/commons:commons_lang3",
        "//dependencies/maven/artifacts/org/ccil/cowan/tagsoup:tagsoup",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy_xml",
        ":rest_assured_common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


