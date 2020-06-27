load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "resolution-lib",
    srcs = [
        "Resolution.java",
        ],
    visibility = ["//visibility:public"],
    deps = [
        "//test/behaviour/resolution/framework/complete",
        "//test/behaviour/resolution/framework/resolve",
        "//test/behaviour/resolution/framework/common",
        "//kb/server:server",
        "//concept/answer:answer",
        "@graknlabs_graql//java:graql",
    ],
)

java_binary(
    name = "resolution",
    main_class = "grakn.verification.resolution.framework.Resolution",
    runtime_deps = [":resolution-lib"],
)

java_binary(
    name = "resolution-debug",
    jvm_flags = [
        "-Xdebug",
        "-Xrunjdwp:transport=dt_socket,server=y,address=5005",
    ],
    main_class = "grakn.verification.resolution.framework.Resolution",
        runtime_deps = [":resolution-lib"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":resolution-lib",
    ],
)