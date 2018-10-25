java_test(
    name = "grakn-graql-commands-e2e",
    test_class = "ai.grakn.distribution.GraknGraqlCommandsE2E",
    srcs = [
        "//test-end-to-end/distribution:GraknGraqlCommandsE2E.java",
        "//test-end-to-end/distribution:DistributionE2E_Constants.java"
        ],
    deps = [
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec"
    ],
    data = ["//:distribution-all"]
)