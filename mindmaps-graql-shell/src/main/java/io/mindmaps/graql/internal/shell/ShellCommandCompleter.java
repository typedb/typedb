package io.mindmaps.graql.internal.shell;

import io.mindmaps.graql.api.shell.GraqlShell;
import jline.console.completer.Completer;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Completer that complete Graql shell commands
 */
public class ShellCommandCompleter implements Completer {

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        Set<String> commands = Stream.of(GraqlShell.COMMANDS)
                .filter(command -> command.startsWith(buffer))
                .collect(Collectors.toSet());

        if (commands.isEmpty()) {
            return cursor;
        } else {
            candidates.addAll(commands);
            return 0;
        }
    }
}
