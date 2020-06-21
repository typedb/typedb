package(default_visibility = ["@graknlabs_grakn_core//__subpackages__"])

load("@graknlabs_bazel_distribution//common:rules.bzl", "java_deps")

alias(
    name = "conf/logback.xml",
    actual = "@graknlabs_console//config/logback:logback.xml",
    visibility = ["//:__pkg__"],
)

java_library(
    name = "cli",
    srcs = glob(["*.java"]),
    deps = [
#        "//daemon:daemon",
#        "//server:server",
        "//dependencies/maven/artifacts/info/picocli:picocli",
#        "@graknlabs_console//:console",
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
    ],
    tags = ["maven_coordinates=io.grakn.cli:grakn-cli:{pom_version}"],
)

java_binary(
    name = "cli-binary",
    main_class = "grakn.core.cli.GraknCli",
    runtime_deps = ["cli"],
    tags = ["maven_coordinates=io.grakn.cli:grakn-cli-binary:{pom_version}"],
)

java_deps(
    name = "cli-deps",
    target = ":cli-binary",
    java_deps_root = "cli/services/lib/",
    visibility = ["//:__pkg__"],
    maven_name = True,
)