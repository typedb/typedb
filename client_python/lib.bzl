def _grakn_py_merge_sourcetrees_impl(ctx):
    args = [ctx.outputs.out.path] + [f.path for f in ctx.files.py_src + ctx.files.proto_src]
    ctx.actions.run(inputs = ctx.attr.py_src.files + ctx.attr.proto_src.files,
        outputs = [ctx.outputs.out],
        arguments = args,
        progress_message = "Merging source trees into %s" % ctx.outputs.out.short_path,
        executable = ctx.executable._merger_script
    )
    return [DefaultInfo(runfiles = ctx.runfiles(files=[ctx.outputs.out]))]


grakn_py_merge_sourcetrees = rule(
    implementation = _grakn_py_merge_sourcetrees_impl,
    attrs = {
        "py_src": attr.label(),
        "proto_src": attr.label(),
        "_merger_script": attr.label(
            default = Label("//client_python:grakn_merge_sourcetrees"),
            cfg="host",
            executable = True,
        )
    },
    outputs = {
        "out": "%{name}.zip"
    }
)
