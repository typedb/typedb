java_library(
    name = "json_path",
    exports = [
        "//external:jar/com/jayway/restassured/json_path"
    ],
    runtime_deps = [
        "//dependencies/maven/org/codehaus/groovy:groovy",
        "//dependencies/maven/org/codehaus/groovy:groovy_json",
        ":rest_assured_common"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "rest_assured",
    exports = [
        "//external:jar/com/jayway/restassured/rest_assured"
    ],
    runtime_deps = [
        "//dependencies/maven/org/apache/httpcomponents:httpclient",
        "//dependencies/maven/org/apache/httpcomponents:httpmime",
        "//dependencies/maven/org/ccil/cowan/tagsoup:tagsoup",
        "//dependencies/maven/org/codehaus/groovy:groovy",
        "//dependencies/maven/org/codehaus/groovy:groovy_xml",
        "//dependencies/maven/org/hamcrest:hamcrest_core",
        "//dependencies/maven/org/hamcrest:hamcrest_library",
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
        "//dependencies/maven/org/apache/commons:commons_lang3",
        "//dependencies/maven/org/codehaus/groovy:groovy"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)



java_library(
    name = "xml_path",
    exports = [
        "//external:jar/com/jayway/restassured/xml_path"
    ],
    runtime_deps = [
        "//dependencies/maven/org/apache/commons:commons_lang3",
        "//dependencies/maven/org/ccil/cowan/tagsoup:tagsoup",
        "//dependencies/maven/org/codehaus/groovy:groovy",
        "//dependencies/maven/org/codehaus/groovy:groovy_xml",
        ":rest_assured_common"
    ],
    visibility = [
        "//dependencies/maven:__subpackages__"
    ]
)


