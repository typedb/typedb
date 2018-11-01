def _distribution_impl(ctx):
    # look at ctx distribution and
    print(ctx.file.version.path)
    ctx.actions.run_shell(
        inputs = [ctx.file.version, ctx.file.filename],
        outputs = [ctx.outputs.distribution_script],
        command = "echo %s-`cat %s`.zip >> %s" %
            (ctx.attr.filename, ctx.file.version.path, ctx.outputs.distribution_script.path)
    )
    return DefaultInfo(
        executable = ctx.outputs.distribution_script,
    )

distribution = rule(
    attrs = {
        "version": attr.label(allow_single_file = True, mandatory = True),
        "filename": attr.label(allow_single_file = True, mandatory = True),
    },
    executable = True,
    outputs = {
        "distribution_script": "%{name}.sh"
    },
    implementation = _distribution_impl
)