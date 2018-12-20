def checkstyle_repositories(
    omit = [],
    versions = {
      "antlr_antlr": "2.7.7",
      "org_antlr_antlr4_runtime": "4.5.1-1",
      "com_puppycrawl_tools_checkstyle": "8.15",
      "commons_beanutils_commons_beanutils": "1.9.3",
      "info_picocli_picocli": "3.8.2",
      "commons_collections_commons_collections": "3.2.2",
      "com_google_guava_guava23": "23.0",
      "org_slf4j_slf4j_api": "1.7.7",
      "org_slf4j_slf4j_jcl": "1.7.7",
    }
):
  if not "antlr_antlr" in omit:
    native.maven_jar(
        name = "antlr_antlr",
        artifact = "antlr:antlr:" + versions["antlr_antlr"],
    )
  if not "org_antlr_antlr4_runtime" in omit:
    native.maven_jar(
        name = "org_antlr_antlr4_runtime",
        artifact = "org.antlr:antlr4-runtime:" + versions["org_antlr_antlr4_runtime"],
    )
  if not "com_puppycrawl_tools_checkstyle" in omit:
    native.maven_jar(
        name = "com_puppycrawl_tools_checkstyle",
        artifact = "com.puppycrawl.tools:checkstyle:" + versions["com_puppycrawl_tools_checkstyle"],
    )
  if not "commons_beanutils_commons_beanutils" in omit:
    native.maven_jar(
        name = "commons_beanutils_commons_beanutils",
        artifact = "commons-beanutils:commons-beanutils:" + versions["commons_beanutils_commons_beanutils"],
    )
  if not "info_picocli_picocli" in omit:
    native.maven_jar(
        name = "info_picocli_picocli",
        artifact = "info.picocli:picocli:" + versions["info_picocli_picocli"],
    )
  if not "commons_collections_commons_collections" in omit:
    native.maven_jar(
        name = "commons_collections_commons_collections",
        artifact = "commons-collections:commons-collections:" + versions["commons_collections_commons_collections"],
    )
  if not "com_google_guava_guava23" in omit:
    native.maven_jar(
        name = "com_google_guava_guava23",
        artifact = "com.google.guava:guava:" + versions["com_google_guava_guava23"],
    )
  if not "org_slf4j_slf4j_api" in omit:
    native.maven_jar(
        name = "org_slf4j_slf4j_api",
        artifact = "org.slf4j:slf4j-api:" + versions["org_slf4j_slf4j_api"],
    )
  if not "org_slf4j_slf4j_jcl" in omit:
    native.maven_jar(
        name = "org_slf4j_slf4j_jcl",
        artifact = "org.slf4j:jcl-over-slf4j:" + versions["org_slf4j_slf4j_jcl"],
    )


JavaSourceFiles = provider(
    fields = {
        'files' : 'java source files'
    }
)


def collect_sources_impl(target, ctx):
    files = []
    if hasattr(ctx.rule.attr, 'srcs'):
        for src in ctx.rule.attr.srcs:
            for f in src.files:
                if f.extension == 'java':
                    files.append(f)
    return [JavaSourceFiles(files = files)]


collect_sources = aspect(
    implementation = collect_sources_impl,
)


def _checkstyle_test_impl(ctx):
    if "{}-checkstyle".format(ctx.attr.target.label.name) != ctx.attr.name:
        fail("target should follow `{java_library target name}-checkstyle` pattern")
    properties = ctx.file.properties
    suppressions = ctx.file.suppressions
    opts = ctx.attr.opts
    sopts = ctx.attr.string_opts

    classpath = ":".join([file.path for file in ctx.files._classpath])

    args = ""
    inputs = []
    if ctx.file.config:
      args += " -c %s" % ctx.file.config.path
      inputs.append(ctx.file.config)
    if properties:
      args += " -p %s" % properties.path
      inputs.append(properties)
    if suppressions:
      inputs.append(suppressions)

    cmd = " ".join(
        ["java -cp %s com.puppycrawl.tools.checkstyle.Main" % classpath] +
        [args] +
        ["--%s" % x for x in opts] +
        ["--%s %s" % (k, sopts[k]) for k in sopts] +
        [file.path for file in ctx.attr.target[JavaSourceFiles].files]
    )

    ctx.actions.expand_template(
        template = ctx.file._checkstyle_py_template,
        output = ctx.outputs.checkstyle_script,
        substitutions = {
            "{command}" : cmd,
            "{allow_failure}": str(int(ctx.attr.allow_failure)),
        },
        is_executable = True,
    )

    files = [ctx.outputs.checkstyle_script, ctx.file.license] + ctx.attr.target[JavaSourceFiles].files + ctx.files._classpath + inputs
    runfiles = ctx.runfiles(
        files = files,
        collect_data = True
    )
    return DefaultInfo(
        executable = ctx.outputs.checkstyle_script,
        files = depset(files),
        runfiles = runfiles,
    )

checkstyle_test = rule(
    implementation = _checkstyle_test_impl,
    test = True,
    attrs = {
        "config": attr.label(
            allow_single_file=True,
            doc = "A checkstyle configuration file"
        ),
        "suppressions": attr.label(
            allow_single_file=True,
            doc = "A checkstyle suppressions file"
        ),
        "license": attr.label(
            allow_single_file=True,
            doc = "A license file that can be used with the checkstyle license target"
        ),
        "properties": attr.label(
            allow_single_file=True,
            doc = "A properties file to be used"
        ),
        "opts": attr.string_list(
            doc = "Options to be passed on the command line that have no argument"
        ),
        "string_opts": attr.string_dict(
            doc = "Options to be passed on the command line that have an argument"
        ),
        "target": attr.label(
            doc = "The java_library target to check sources on",
            aspects = [collect_sources]
        ),
        "allow_failure": attr.bool(
            default = False,
            doc = "Successfully finish the test even if checkstyle failed"
        ),
        "_checkstyle_py_template": attr.label(
             allow_files = True,
             single_file = True,
             default = "//dependencies/tools/checkstyle/templates:checkstyle.py"
        ),
        "_classpath": attr.label_list(default=[
            Label("@com_puppycrawl_tools_checkstyle//jar"),
            Label("@commons_beanutils_commons_beanutils//jar"),
            Label("@info_picocli_picocli//jar"),
            Label("@commons_collections_commons_collections//jar"),
            Label("@org_slf4j_slf4j_api//jar"),
            Label("@org_slf4j_slf4j_jcl//jar"),
            Label("@antlr_antlr//jar"),
            Label("@org_antlr_antlr4_runtime//jar"),
            Label("@com_google_guava_guava23//jar"),
        ]),
    },
    outputs = {
        "checkstyle_script": "%{name}.py",
    },
)
