# Do not edit. bazel-deps autogenerates this file from dependencies/maven/dependencies.yaml.
def _jar_artifact_impl(ctx):
    jar_name = "%s.jar" % ctx.name
    ctx.download(
        output=ctx.path("jar/%s" % jar_name),
        url=ctx.attr.urls,
        sha256=ctx.attr.sha256,
        executable=False
    )
    src_name="%s-sources.jar" % ctx.name
    srcjar_attr=""
    has_sources = len(ctx.attr.src_urls) != 0
    if has_sources:
        ctx.download(
            output=ctx.path("jar/%s" % src_name),
            url=ctx.attr.src_urls,
            sha256=ctx.attr.src_sha256,
            executable=False
        )
        srcjar_attr ='\n    srcjar = ":%s",' % src_name

    build_file_contents = """
package(default_visibility = ['//visibility:public'])
java_import(
    name = 'jar',
    tags = ['maven_coordinates={artifact}'],
    jars = ['{jar_name}'],{srcjar_attr}
)
filegroup(
    name = 'file',
    srcs = [
        '{jar_name}',
        '{src_name}'
    ],
    visibility = ['//visibility:public']
)\n""".format(artifact = ctx.attr.artifact, jar_name = jar_name, src_name = src_name, srcjar_attr = srcjar_attr)
    ctx.file(ctx.path("jar/BUILD"), build_file_contents, False)
    return None

jar_artifact = repository_rule(
    attrs = {
        "artifact": attr.string(mandatory = True),
        "sha256": attr.string(mandatory = True),
        "urls": attr.string_list(mandatory = True),
        "src_sha256": attr.string(mandatory = False, default=""),
        "src_urls": attr.string_list(mandatory = False, default=[]),
    },
    implementation = _jar_artifact_impl
)

def jar_artifact_callback(hash):
    src_urls = []
    src_sha256 = ""
    source=hash.get("source", None)
    if source != None:
        src_urls = [source["url"]]
        src_sha256 = source["sha256"]
    jar_artifact(
        artifact = hash["artifact"],
        name = hash["name"],
        urls = [hash["url"]],
        sha256 = hash["sha256"],
        src_urls = src_urls,
        src_sha256 = src_sha256
    )
    native.bind(name = hash["bind"], actual = hash["actual"])


def list_dependencies():
    return [
    {"artifact": "com.google.code.findbugs:jsr305:3.0.2", "lang": "java", "sha1": "25ea2e8b0c338a877313bd4672d3fe056ea78f0d", "sha256": "766ad2a0783f2687962c8ad74ceecc38a28b9f72a2d085ee438b7813e928d0c7", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar", "source": {"sha1": "b19b5927c2c25b6c70f093767041e641ae0b1b35", "sha256": "1c9e85e272d0708c6a591dc74828c71603053b48cc75ae83cce56912a2aa063b", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2-sources.jar"} , "name": "com-google-code-findbugs-jsr305", "actual": "@com-google-code-findbugs-jsr305//jar", "bind": "jar/com/google/code/findbugs/jsr305"},
    {"artifact": "io.cucumber:cucumber-core:5.1.3", "lang": "java", "sha1": "1e8d72dd9f251085f000ab83963aaf14870f751e", "sha256": "e8a2121592e9696850bda2fb96b0cc3fe403fe9e1114aa5d483ec0e956721f88", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-core/5.1.3/cucumber-core-5.1.3.jar", "source": {"sha1": "f4ace11bfd28e60a242ed853d6237762ad0919ac", "sha256": "580bd656f9d82adad7efee980671bd5885d55849ee437c84dfea2239328ba2c8", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-core/5.1.3/cucumber-core-5.1.3-sources.jar"} , "name": "io-cucumber-cucumber-core", "actual": "@io-cucumber-cucumber-core//jar", "bind": "jar/io/cucumber/cucumber-core"},
    {"artifact": "io.cucumber:cucumber-expressions:8.3.1", "lang": "java", "sha1": "5d885415d050be6cd197afa726a4a4733cb50a75", "sha256": "1d321a1f43e752467a1f40c167f2b840594237c32888c84c7e67b10fe7d03c3d", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-expressions/8.3.1/cucumber-expressions-8.3.1.jar", "source": {"sha1": "c9153d5f6f48c84a6efeaf6affb648a93d522df8", "sha256": "3bac153b015532505af4a509e42973eec7fe9c5ec5668986559c151f460d2e65", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-expressions/8.3.1/cucumber-expressions-8.3.1-sources.jar"} , "name": "io-cucumber-cucumber-expressions", "actual": "@io-cucumber-cucumber-expressions//jar", "bind": "jar/io/cucumber/cucumber-expressions"},
    {"artifact": "io.cucumber:cucumber-gherkin-vintage:5.1.3", "lang": "java", "sha1": "9d03ceecba60baa2985e76da57a990be6dc77e3e", "sha256": "c163ca08002de259fe35cceed0de1ed7c8ae7c96c40793803d8a144ab5a5448c", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-gherkin-vintage/5.1.3/cucumber-gherkin-vintage-5.1.3.jar", "source": {"sha1": "106b8b1ef81a3e0966fb040d75e3d605e0b8b6d8", "sha256": "f429af36c30c8b4af250b416c1fefc08e1f60cd72c8b8fd21be60bd11394c166", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-gherkin-vintage/5.1.3/cucumber-gherkin-vintage-5.1.3-sources.jar"} , "name": "io-cucumber-cucumber-gherkin-vintage", "actual": "@io-cucumber-cucumber-gherkin-vintage//jar", "bind": "jar/io/cucumber/cucumber-gherkin-vintage"},
    {"artifact": "io.cucumber:cucumber-gherkin:5.1.3", "lang": "java", "sha1": "088c96fa7a5b46e73b85f766762e2eee7841dc33", "sha256": "5487aca9f5ce53680f0050909bc501d78ea46ba4dc9db17ef6609cfb9e7abdca", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-gherkin/5.1.3/cucumber-gherkin-5.1.3.jar", "source": {"sha1": "d07421aa4a0efbd7a854a8f8f6e2d414f6c4b673", "sha256": "120201b14fd6fbae91b7734790aca3e0af58c7b006f6f97d2a7e024995ef4d94", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-gherkin/5.1.3/cucumber-gherkin-5.1.3-sources.jar"} , "name": "io-cucumber-cucumber-gherkin", "actual": "@io-cucumber-cucumber-gherkin//jar", "bind": "jar/io/cucumber/cucumber-gherkin"},
    {"artifact": "io.cucumber:cucumber-java:5.1.3", "lang": "java", "sha1": "fd624244e0811c514e808273f2a69c94c33d15ad", "sha256": "328d07268fe0d763e48c640e5a3740ab3f5e2d33d79d83a8f63a860aef800afd", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-java/5.1.3/cucumber-java-5.1.3.jar", "source": {"sha1": "cf268969fdc72a100c3fff0fd50dbb93b7bc7ef9", "sha256": "0599fdce2e97ec1199cfa5d0bb0e690fe9a967903349d9e061cc02e5e7c0cd07", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-java/5.1.3/cucumber-java-5.1.3-sources.jar"} , "name": "io-cucumber-cucumber-java", "actual": "@io-cucumber-cucumber-java//jar", "bind": "jar/io/cucumber/cucumber-java"},
    {"artifact": "io.cucumber:cucumber-junit:5.1.3", "lang": "java", "sha1": "6affdf0a1946433551e5e042619522ffa49ee74b", "sha256": "c37a1d164aeb79d3c35048aeb0d0e802b894b4cf5b0c3f47fa4ee64c884bb289", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-junit/5.1.3/cucumber-junit-5.1.3.jar", "source": {"sha1": "1ca809e0f051684db4a2263a2e168d8b752f360d", "sha256": "9fc2f1135f8d455d8a598811b09c638d0f293fbc0e84efdb82d0107a47d83ec1", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-junit/5.1.3/cucumber-junit-5.1.3-sources.jar"} , "name": "io-cucumber-cucumber-junit", "actual": "@io-cucumber-cucumber-junit//jar", "bind": "jar/io/cucumber/cucumber-junit"},
    {"artifact": "io.cucumber:cucumber-plugin:5.1.3", "lang": "java", "sha1": "6b7320edf3c6c945d3833eef282a8d6037620754", "sha256": "9e571121810d3cfbd62efeace674031be30e85602b05e9f1ca052375d7992969", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-plugin/5.1.3/cucumber-plugin-5.1.3.jar", "source": {"sha1": "bef7cc599bff12cba069126ab7bd4d583b766476", "sha256": "a6b71c8ca5e5b48f9ebfd097a26380e1d005c58c313155789911ea5c57756477", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/cucumber-plugin/5.1.3/cucumber-plugin-5.1.3-sources.jar"} , "name": "io-cucumber-cucumber-plugin", "actual": "@io-cucumber-cucumber-plugin//jar", "bind": "jar/io/cucumber/cucumber-plugin"},
    {"artifact": "io.cucumber:datatable:3.2.1", "lang": "java", "sha1": "27dc193cae83e641478b38b583ed97d5d944a12c", "sha256": "2f6e3964f3315f92e7df38433c39f75fa0d4fca1445edd203c1ddf3c9665e466", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/datatable/3.2.1/datatable-3.2.1.jar", "source": {"sha1": "a0a524a983c2f2f142c81d573c26aec224ec5e31", "sha256": "2d57ad32760e4b8d02b3ae767d88d82c206f59610a043844d53ad33df22df7da", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/datatable/3.2.1/datatable-3.2.1-sources.jar"} , "name": "io-cucumber-datatable", "actual": "@io-cucumber-datatable//jar", "bind": "jar/io/cucumber/datatable"},
    {"artifact": "io.cucumber:docstring:5.1.3", "lang": "java", "sha1": "15688c174ab5e956cca475d55d7e9ef16eec5919", "sha256": "d0a5eb9544cf3075a00abbb8eb48f38ffd1c5741057f4b078c59dd2a3cd13016", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/docstring/5.1.3/docstring-5.1.3.jar", "source": {"sha1": "c785f5fe7b071e366dadfa6089274b3a252e4472", "sha256": "2aeb0d5dbcd9a061f51e80dc6b430558097cac48ec6fd26b3e7a559b868690ea", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/docstring/5.1.3/docstring-5.1.3-sources.jar"} , "name": "io-cucumber-docstring", "actual": "@io-cucumber-docstring//jar", "bind": "jar/io/cucumber/docstring"},
    {"artifact": "io.cucumber:tag-expressions:2.0.4", "lang": "java", "sha1": "b0587c9db1794f90b680b2b422c8df6170b384c1", "sha256": "17d14d224116886bfbea5e322509edeea7e19cbc931bb616f384abd859b0a3da", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/tag-expressions/2.0.4/tag-expressions-2.0.4.jar", "source": {"sha1": "37cf1661bf5b6a18510da63bf4bf0c82f2613da9", "sha256": "264da74f58c54095f9f0ac34898441470aa20f0bb0f7b7c3dcf9e271713b2ef6", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/io/cucumber/tag-expressions/2.0.4/tag-expressions-2.0.4-sources.jar"} , "name": "io-cucumber-tag-expressions", "actual": "@io-cucumber-tag-expressions//jar", "bind": "jar/io/cucumber/tag-expressions"},
    {"artifact": "junit:junit:4.12", "lang": "java", "sha1": "2973d150c0dc1fefe998f834810d68f278ea58ec", "sha256": "59721f0805e223d84b90677887d9ff567dc534d7c502ca903c0c2b17f05c116a", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/junit/junit/4.12/junit-4.12.jar", "source": {"sha1": "a6c32b40bf3d76eca54e3c601e5d1470c86fcdfa", "sha256": "9f43fea92033ad82bcad2ae44cec5c82abc9d6ee4b095cab921d11ead98bf2ff", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/junit/junit/4.12/junit-4.12-sources.jar"} , "name": "junit-junit", "actual": "@junit-junit//jar", "bind": "jar/junit/junit"},
    {"artifact": "org.apiguardian:apiguardian-api:1.1.0", "lang": "java", "sha1": "fc9dff4bb36d627bdc553de77e1f17efd790876c", "sha256": "a9aae9ff8ae3e17a2a18f79175e82b16267c246fbbd3ca9dfbbb290b08dcfdd4", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/apiguardian/apiguardian-api/1.1.0/apiguardian-api-1.1.0.jar", "source": {"sha1": "f3c15fe970af864390c8d0634c9f16aca1b064a8", "sha256": "d39a5bb9b4b57e7584ac81f714ba8ef73b08ca462a48d7828d4a93fa5013fe1e", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/apiguardian/apiguardian-api/1.1.0/apiguardian-api-1.1.0-sources.jar"} , "name": "org-apiguardian-apiguardian-api", "actual": "@org-apiguardian-apiguardian-api//jar", "bind": "jar/org/apiguardian/apiguardian-api"},
    {"artifact": "org.hamcrest:hamcrest-core:1.3", "lang": "java", "sha1": "42a25dc3219429f0e5d060061f71acb49bf010a0", "sha256": "66fdef91e9739348df7a096aa384a5685f4e875584cce89386a7a47251c4d8e9", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar", "source": {"sha1": "1dc37250fbc78e23a65a67fbbaf71d2e9cbc3c0b", "sha256": "e223d2d8fbafd66057a8848cc94222d63c3cedd652cc48eddc0ab5c39c0f84df", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3-sources.jar"} , "name": "org-hamcrest-hamcrest-core", "actual": "@org-hamcrest-hamcrest-core//jar", "bind": "jar/org/hamcrest/hamcrest-core"},
    {"artifact": "org.rocksdb:rocksdbjni:6.6.4", "lang": "java", "sha1": "c0a678885999bdca1bca8ee1226ecc2b9aec7596", "sha256": "a71f1afaa127f7b940cfa81877d6c30a524d3068f469918320e035946f54f71a", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/rocksdb/rocksdbjni/6.6.4/rocksdbjni-6.6.4.jar", "source": {"sha1": "fd184e9a85a7794880759faf82ecee43fd74a12d", "sha256": "3b64d27d6925777428bcf6562f9e436d925b78e90607677e697971c156ad1122", "repository": "https://repo.maven.apache.org/maven2/", "url": "https://repo.maven.apache.org/maven2/org/rocksdb/rocksdbjni/6.6.4/rocksdbjni-6.6.4-sources.jar"} , "name": "org-rocksdb-rocksdbjni", "actual": "@org-rocksdb-rocksdbjni//jar", "bind": "jar/org/rocksdb/rocksdbjni"},
    ]

def maven_dependencies(callback = jar_artifact_callback):
    for hash in list_dependencies():
        callback(hash)
