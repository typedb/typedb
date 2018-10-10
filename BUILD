java_binary(
    name = "console-build-bin",
    main_class = "ai.grakn.core.console.Graql",
    runtime_deps = ["//console:console"]
)

genrule(
    name = "console-build-dist",
    srcs = ["grakn-core", "console-build-bin_deploy.jar", "//console:conf/logback.xml"],
    outs = ["console.zip"],
    cmd  = "$(location console-packager.sh) $(location grakn-core) $(location console-build-bin_deploy.jar) $(location //console:conf/logback.xml) $(location console.zip)",
    tools = ["console-packager.sh"]
)

java_binary(
    name = "server-build-bin",
    main_class = "ai.grakn.core.server.bootup.GraknBootup",
    runtime_deps = ["//server:server"]
)

genrule(
    name = "server-build-dist",
    srcs = ["grakn-core", "server-build-bin_deploy.jar", "//server:conf/grakn.properties", "//server:conf/logback.xml"],
    outs = ["server.zip"],
    cmd  = "$(location server-packager.sh) $(location grakn-core) $(location server-build-bin_deploy.jar) $(location //server:conf/grakn.properties) $(location //server:conf/logback.xml) $(location server.zip)",
    tools = ["server-packager.sh"]
)