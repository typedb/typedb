package grakn.core.dependencies.maven

import java.nio.file.Files
import java.nio.file.Paths
import java.util.stream.Collectors

fun main() {
    val BASE_DIR=Paths.get(System.getenv("BUILD_WORKSPACE_DIRECTORY"))
    val target = BASE_DIR.resolve("dependencies").resolve("maven").resolve("snapshot")
    val process = ProcessBuilder()
            .directory(BASE_DIR.toFile())
            .command("bazel", "query", "@maven//...")
            .start()
    val output = process.inputStream
            .use { output -> output.reader().readLines() }
            .stream().sorted().collect(Collectors.joining(System.lineSeparator()))
    Files.write(target, output.toByteArray())
}