java_library(
    name = "jffi",
    exports = [
        "//external:jar/com/github/jnr/jffi"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jffi_jar_native",
    exports = [
        "//external:jar/com/github/jnr/jffi_jar_native"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jnr_constants",
    exports = [
        "//external:jar/com/github/jnr/jnr_constants"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jnr_ffi",
    exports = [
        "//external:jar/com/github/jnr/jnr_ffi"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/ow2/asm:asm",
        "//dependencies/maven/artifacts/org/ow2/asm:asm_analysis",
        "//dependencies/maven/artifacts/org/ow2/asm:asm_commons",
        "//dependencies/maven/artifacts/org/ow2/asm:asm_tree",
        "//dependencies/maven/artifacts/org/ow2/asm:asm_util",
        ":jffi",
        ":jffi_jar_native",
        ":jnr_x86asm"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jnr_posix",
    exports = [
        "//external:jar/com/github/jnr/jnr_posix"
    ],
    runtime_deps = [
        ":jnr_constants",
        ":jnr_ffi"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)



java_library(
    name = "jnr_x86asm",
    exports = [
        "//external:jar/com/github/jnr/jnr_x86asm"
    ],
    visibility = [
        "//dependencies/maven/artifacts:__subpackages__"
    ]
)


