java_library(
    name = "json-path",
    exports = [
        "//external:jar/com/jayway/restassured/json-path"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy-json",
        ":rest-assured-common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "rest-assured",
    exports = [
        "//external:jar/com/jayway/restassured/rest-assured"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apache/httpcomponents:httpclient",
        "//dependencies/maven/artifacts/org/apache/httpcomponents:httpmime",
        "//dependencies/maven/artifacts/org/ccil/cowan/tagsoup:tagsoup",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy-xml",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        ":json-path",
        ":xml-path"
    ],
    visibility = [
        "//visibility:public"
    ]
)



java_library(
    name = "rest-assured-common",
    exports = [
        "//external:jar/com/jayway/restassured/rest-assured-common"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "xml-path",
    exports = [
        "//external:jar/com/jayway/restassured/xml-path"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/ccil/cowan/tagsoup:tagsoup",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy",
        "//dependencies/maven/artifacts/org/codehaus/groovy:groovy-xml",
        ":rest-assured-common"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


