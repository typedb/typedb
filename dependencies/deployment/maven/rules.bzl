#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

load(
    "@com_github_google_bazel_common//tools/maven:pom_file.bzl",
    pom_file_exports = "exports",
)

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

def _generate_pom_xml(ctx, maven_coordinates):
    # Final 'pom.xml' is generated in 2 steps
    preprocessed_template = ctx.actions.declare_file("_pom.xml")

    # Prepare a fake context object for rule execution
    mock_ctx = struct(
        attr = struct(
            targets = ctx.attr.targets,
            preferred_group_ids = [],
            excluded_artifacts = [],
            substitutions = {
                "{group_id}": maven_coordinates.group_id,
                "{artifact_id}": maven_coordinates.artifact_id,
            }
        ),
        var = {
            "pom_version": "{pom_version}"
        },
        actions = ctx.actions,
        file = struct(
            template_file = ctx.file._pom_xml_template
        ),
        outputs = struct(
            pom_file = preprocessed_template
        )
    )

    # Step 1: fill in everything except version using `pom_file` rule implementation
    pom_file_exports._pom_file(mock_ctx)

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
    # Perform input checks first
    if (len(ctx.attr.targets) != 1):
        fail("should specify single jar to deploy")

    target = ctx.attr.targets[0]

    if (len(target.java.outputs.jars) != 1):
        fail("should specify rule that produces a single jar")

    target_string = target[pom_file_exports.MavenInfo].maven_artifacts.to_list()[0]

    maven_coordinates = _parse_maven_coordinates(target_string)

    _generate_pom_xml(ctx, maven_coordinates)
    _generate_deployment_script(ctx, maven_coordinates)

    # there is also .source_jar which produces '.srcjar'
    jar = target.java.outputs.jars[0].class_jar

    return DefaultInfo(executable = ctx.outputs.deployment_script,
        runfiles = ctx.runfiles(
            files=[jar, ctx.outputs.pom_file, ctx.file._deployment_properties],
            # generate symlinks with predictable names
            symlinks={
                "lib.jar": jar,
                "pom.xml": ctx.outputs.pom_file,
                "deployment.properties": ctx.file._deployment_properties
            }))

deploy_maven_jar = rule(
    attrs = {
        "targets": attr.label_list(
            mandatory = True,
            aspects = [pom_file_exports._collect_maven_info],
        ),
        "version_file": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "_pom_xml_template": attr.label(
            allow_single_file = True,
            default = "//dependencies/deployment/maven:pom_template.xml",
        ),
        "_deployment_script_template": attr.label(
            allow_single_file = True,
            default = "//dependencies/deployment/maven:deploy.sh",
        ),
        "_deployment_properties": attr.label(
            allow_single_file = True,
            default = "//:deployment.properties"
        )
    },
    executable = True,
    outputs = {
        "pom_file": "%{name}.xml",
        "deployment_script": "%{name}.sh",
    },
    implementation = _deploy_maven_jar_impl,
)
