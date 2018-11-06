java_library(
    name = "auto-value",
    exported_plugins = [
        ":auto-value-plugin"
    ],
    exports = [
        "//external:jar/com/google/auto/value/auto-value"
    ],
    visibility = [
        "//visibility:public"
    ]
)
java_plugin(
    name = "auto-value-plugin",
    deps = [
        "//external:jar/com/google/auto/value/auto-value"
    ],
    processor_class = "com.google.auto.value.processor.AutoValueProcessor",
    visibility = [
        "//visibility:public"
    ]
)



