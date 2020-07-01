package grakn.core.dependencies.maven

import java.nio.file.Files
import java.nio.file.Paths

fun main() {
    val BASE_DIR=System.getenv("BUILD_WORKSPACE_DIRECTORY")
    val jsonSrc = Paths.get(BASE_DIR, "maven_install.json")
    val jsonTarget = Paths.get(BASE_DIR, "dependencies", "maven", "snapshot.json")
    ProcessBuilder()
            .directory(Paths.get(BASE_DIR).toFile())
            .command("bazel", "run", "@maven//:pin")
            .start()
            .waitFor()

    Files.move(jsonSrc, jsonTarget)
}