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
        "//resolution/complete",
        "//resolution/resolve",
        "//resolution/common",
        "@graknlabs_client_java//:client-java",
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