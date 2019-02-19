# Do not edit. deployment_rules_builder.py autogenerates this file from deployment/maven/templates/rules.bzl

# Imported from google/bazel-commons: begin
MavenInfo = provider(
    fields = {
        "maven_artifacts": """
        The Maven coordinates for the artifacts that are exported by this target: i.e. the target
        itself and its transitively exported targets.
        """,
        "maven_dependencies": """
        The Maven coordinates of the direct dependencies, and the transitively exported targets, of
        this target.
        """,
    },
)

_EMPTY_MAVEN_INFO = MavenInfo(
    maven_artifacts = depset(),
    maven_dependencies = depset(),
)

_MAVEN_COORDINATES_PREFIX = "maven_coordinates="

def _maven_artifacts(targets):
    return [target[MavenInfo].maven_artifacts for target in targets if MavenInfo in target]

def _collect_maven_info_impl(_target, ctx):
    tags = getattr(ctx.rule.attr, "tags", [])
    deps = getattr(ctx.rule.attr, "deps", [])
    runtime_deps = getattr(ctx.rule.attr, "runtime_deps", [])
    exports = getattr(ctx.rule.attr, "exports", [])

    maven_artifacts = []
    for tag in tags:
        if tag in ("maven:compile_only", "maven:shaded"):
            return [_EMPTY_MAVEN_INFO]
        if tag.startswith(_MAVEN_COORDINATES_PREFIX):
            maven_artifacts.append(tag[len(_MAVEN_COORDINATES_PREFIX):])

    return [MavenInfo(
        maven_artifacts = depset(maven_artifacts, transitive = _maven_artifacts(exports)),
        maven_dependencies = depset([], transitive = _maven_artifacts(deps + exports + runtime_deps)),
    )]

_collect_maven_info = aspect(
    attr_aspects = [
        "jars",
        "deps",
        "exports",
        "runtime_deps"
    ],
    doc = """
    Collects the Maven information for targets, their dependencies, and their transitive exports.
    """,
    implementation = _collect_maven_info_impl,
    provides = [MavenInfo]
)
# Imported from google/bazel-commons: end

def _warn(msg):
    print('{red}{msg}{nc}'.format(red='\033[0;31m', msg=msg, nc='\033[0m'))

def _parse_maven_coordinates(coordinate_string):
    group_id, artifact_id, version = coordinate_string.split(':')
    if version != '{pom_version}':
        fail('should assign {pom_version} as Maven version via `tags` attribute')
    return struct(
        group_id = group_id,
        artifact_id = artifact_id,
    )

TransitiveMavenInfo = provider(
    fields = {
        'transitive_dependencies': 'Maven coordinates for dependencies, transitively collected'
    }
)

DEP_BLOCK = """
<dependency>
  <groupId>{0}</groupId>
  <artifactId>{1}</artifactId>
  <version>{2}</version>
</dependency>
""".strip()

def _generate_pom_xml(ctx, maven_coordinates):
    # Final 'pom.xml' is generated in 2 steps
    preprocessed_template = ctx.actions.declare_file("_pom.xml")

    tags = depset(ctx.attr.target[TransitiveMavenInfo].transitive_dependencies).to_list()

    xml_tags = []
    for tag in tags:
        xml_tags.append(DEP_BLOCK.format(*tag.split(":")))

    # Step 1: fill in everything except version using `pom_file` rule implementation
    ctx.actions.expand_template(
        template = ctx.file._pom_xml_template,
        output = preprocessed_template,
        substitutions = {
            "{group_id}": maven_coordinates.group_id,
            "{artifact_id}": maven_coordinates.artifact_id,
            "{maven_dependencies}": "\n".join(xml_tags)
        }
    )

    # Step 2: fill in {pom_version} from version_file
    ctx.actions.run_shell(
        inputs = [preprocessed_template, ctx.file.version_file],
        outputs = [ctx.outputs.pom_file],
        command = "VERSION=`cat %s` && sed -e s/{pom_version}/$VERSION/g %s > %s" % (
            ctx.file.version_file.path, preprocessed_template.path, ctx.outputs.pom_file.path)
    )

def _generate_deployment_script(ctx, maven_coordinates):
    # Final 'deploy.sh' is generated in 2 steps
    preprocessed_script = ctx.actions.declare_file("_deploy.sh")

    # Maven artifact coordinates split by slash i.e. io/grakn/grakn-graql/grakn-graql
    coordinates = "/".join([
        maven_coordinates.group_id.replace('.', '/'),
        maven_coordinates.artifact_id
    ])

    # Step 1: fill in {pom_version} from version_file
    ctx.actions.run_shell(
        inputs = [ctx.file._deployment_script_template, ctx.file.version_file],
        outputs = [preprocessed_script],
        command = "VERSION=`cat %s` && sed -e s/{pom_version}/$VERSION/g %s > %s" % (
            ctx.file.version_file.path, ctx.file._deployment_script_template.path, preprocessed_script.path)
    )

    # Step 2: fill in everything except version
    ctx.actions.expand_template(
        template = preprocessed_script,
        output = ctx.outputs.deployment_script,
        substitutions = {
            "$ARTIFACT": maven_coordinates.artifact_id,
            "$COORDINATES": coordinates,
        },
        is_executable = True
    )

def _deploy_maven_jar_impl(ctx):
    target = ctx.attr.target

    jars=[]
    for tgt in ctx.attr.deps:
        jars.append(tgt.java.outputs.jars[0].class_jar)

    target_string = target[MavenInfo].maven_artifacts.to_list()[0]

    maven_coordinates = _parse_maven_coordinates(target_string)

    _generate_pom_xml(ctx, maven_coordinates)
    _generate_deployment_script(ctx, maven_coordinates)

    # there is also .source_jar which produces '.srcjar'
    if hasattr(target, "java"):
        jar = target.java.outputs.jars[0].class_jar
    elif hasattr(target, "files"):
        jar = target.files.to_list()[0]
    else:
        fail("Could not find JAR file to deploy in {}".format(target))

    symlinks = {}
    for i, libjar in enumerate(jars):
        symlinks["lib{}.jar".format(i)] = libjar

    symlinks["lib.jar"] = jar
    symlinks["pom.xml"] = ctx.outputs.pom_file
    symlinks["deployment.properties"] = ctx.file.deployment_properties

    return DefaultInfo(executable = ctx.outputs.deployment_script,
        runfiles = ctx.runfiles(
            files=[jar, ctx.outputs.pom_file, ctx.file.deployment_properties] + jars,
            # generate symlinks with predictable names
            symlinks=symlinks))


def _transitive_maven_dependencies(_target, ctx):
    tags = []

    if MavenInfo in _target:
        for x in _target[MavenInfo].maven_dependencies.to_list():
            tags.append(x)

    for dep in getattr(ctx.rule.attr, "jars", []):
        if dep.label.package.startswith(ctx.attr.package):
            tags = tags + dep[TransitiveMavenInfo].transitive_dependencies
    for dep in getattr(ctx.rule.attr, "deps", []):
        if dep.label.package.startswith(ctx.attr.package):
            tags = tags + dep[TransitiveMavenInfo].transitive_dependencies
    for dep in getattr(ctx.rule.attr, "exports", []):
        if dep.label.package.startswith(ctx.attr.package):
            tags = tags + dep[TransitiveMavenInfo].transitive_dependencies
    for dep in getattr(ctx.rule.attr, "runtime_deps", []):
        if dep.label.package.startswith(ctx.attr.package):
            tags = tags + dep[TransitiveMavenInfo].transitive_dependencies
    return [TransitiveMavenInfo(transitive_dependencies = tags)]

# Filled in by deployment_rules_builder
_maven_packages = "common,concept,console,protocol,server,client-java".split(",")
_default_version_file = None if 'version_file_placeholder' in "//:VERSION" else "//:VERSION"
_default_deployment_properties = None if 'deployment_properties_placeholder' in "//:deployment.properties" else "//:deployment.properties"

_transitive_maven_info = aspect(
    attr_aspects = [
        "jars",
        "deps",
        "exports",
        "runtime_deps",
        "extension"
    ],
    required_aspect_providers = [MavenInfo],
    implementation = _transitive_maven_dependencies,
    attrs = {
        "package": attr.string(values = _maven_packages)
    }
)

deploy_maven_jar = rule(
    attrs = {
        "target": attr.label(
            mandatory = True,
            aspects = [
                _collect_maven_info,
                _transitive_maven_info,
            ]
        ),
        "package": attr.string(),
        "deps": attr.label_list(
            mandatory = False,
        ),
        "version_file": attr.label(
            allow_single_file = True,
            mandatory = not bool(_default_version_file),
            default = _default_version_file
        ),
        "deployment_properties": attr.label(
            allow_single_file = True,
            mandatory = not bool(_default_deployment_properties),
            default = _default_deployment_properties
        ),
        "_pom_xml_template": attr.label(
            allow_single_file = True,
            default = "@graknlabs_bazel_distribution//maven/templates:pom.xml",
        ),
        "_deployment_script_template": attr.label(
            allow_single_file = True,
            default = "@graknlabs_bazel_distribution//maven/templates:deploy.sh",
        )
    },
    executable = True,
    outputs = {
        "pom_file": "%{name}.xml",
        "deployment_script": "%{name}.sh",
    },
    implementation = _deploy_maven_jar_impl,
)
