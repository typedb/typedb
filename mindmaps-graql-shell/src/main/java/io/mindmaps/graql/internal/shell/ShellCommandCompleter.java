package io.mindmaps.graql.internal.shell;

import io.mindmaps.graql.api.shell.GraqlShell;

import java.util.stream.Stream;

/**
 * Completer that complete Graql shell commands
 */
public class ShellCommandCompleter implements GraqlCompleter {

    @Override
    public Stream<String> getCandidates(String buffer) {
        // If there is no whitespace entered, the user might be about to type a shell command
        if (!buffer.contains(" ")) {
            return Stream.of(GraqlShell.COMMANDS);
        } else {
            return Stream.empty();
        }
    }
}
