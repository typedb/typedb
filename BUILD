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