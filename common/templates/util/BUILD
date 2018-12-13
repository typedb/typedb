genrule(
    name = "grakn-version",
    srcs = [
        "GraknVersion.java",
        "//:VERSION",
    ],
    cmd = "VERSION=`cat $(location //:VERSION)`;sed -e \"s/VERSION_PLACEHOLDER/$$VERSION/g\" $(location GraknVersion.java) >> $@",
    outs = ["util/GraknVersion.java"],
    visibility = ["//common:__subpackages__"],
)