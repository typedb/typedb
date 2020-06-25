load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "resolution-lib",
    srcs = glob([
        "*.java",
        ]
    ),
#    resource_strip_prefix = "conf/",
#    resources = [
#        "conf/logback.xml",
#    ],
    visibility = ["//visibility:public"],
    deps = [
#        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
#        "//dependencies/maven/artifacts/commons-cli",
#        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
#        "//yaml_tool",
        "//test/behaviour/resolution/complete",
        "//test/behaviour/resolution/resolve",
        "//test/behaviour/resolution/common",
        "//test/rule:grakn-test-server",
        "//kb/server:server",
        "//kb/concept/api:api",
        "//concept/answer:answer",
        "@graknlabs_graql//java:graql",
    ],
)

java_binary(
    name = "resolution",
#    args = [
#        "$(locations //data)",
#        "$(locations //schema)",
#    ],
#    data = [
#        "//data",
#        "//schema",
#    ],
    main_class = "grakn.verification.resolution.Resolution",
    runtime_deps = [":resolution-lib"],
)

java_binary(
    name = "resolution-debug",
    jvm_flags = [
        "-Xdebug",
        "-Xrunjdwp:transport=dt_socket,server=y,address=5005",
    ],
    main_class = "grakn.verification.resolution.Resolution",
        runtime_deps = [":resolution-lib"],
)



java_library(
    name = "steps",
    srcs = [
        "ResolutionSteps.java"
    ],
    deps = [
        # Package dependencies
        "//kb/server",
        "//graql/reasoner",
        "//concept/answer",

        # Grakn Labs dependencies
        "@graknlabs_graql//java:graql",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/junit:junit",
        "//dependencies/maven/artifacts/io/cucumber:cucumber-java",
    ],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":resolution-lib",
        ":steps",
    ],
)