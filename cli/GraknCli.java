package grakn.core.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * The main CLI for the `grakn` command.
 *
 * In order to invoke other Java commands
 */
@Command(
        name = "grakn",
        synopsisSubcommandLabel = "[server|console]",
        mixinStandardHelpOptions = true,
        version = {
                "Version currently unavailable"
        }
)
public class GraknCli {

    @Option(
            names = {"--show-component-load-errors"},
            description = "Show full stack traces for component load errors.",
            hidden = true
    )
    boolean showComponentErrors = false;

    @Command(name = "community", description = "Get more from the Grakn community.")
    public static class Community {
        @Command(description = "Open Grakn on GitHub in your default browser.")
        public void github() throws IOException {
            Desktop.getDesktop().browse(URI.create("https://github.com/graknlabs/grakn"));
        }

        @Command(description = "Open Grakn Discord in your default browser.")
        public void discord() throws IOException {
            Desktop.getDesktop().browse(URI.create("https://discord.com/invite/graknlabs"));
        }
    }

    private static final CommandLine.Help.ColorScheme COLOR_SCHEME = new CommandLine.Help.ColorScheme.Builder()
            .commands    (CommandLine.Help.Ansi.Style.fg_white, CommandLine.Help.Ansi.Style.bold)
            .options     (CommandLine.Help.Ansi.Style.fg_cyan)
            .parameters  (CommandLine.Help.Ansi.Style.fg_yellow)
            .optionParams(CommandLine.Help.Ansi.Style.italic)
            .errors      (CommandLine.Help.Ansi.Style.fg_red, CommandLine.Help.Ansi.Style.bold)
            .stackTraces (CommandLine.Help.Ansi.Style.italic)
            .build();

    public static void main(String[] args) {
        Path graknHome = Paths.get(Objects.requireNonNull(System.getProperty("grakn.dir")));

        GraknComponentLoader loader = new GraknComponentLoader(graknHome);

        // We do an initial pass of the CLI args in case we want to display exceptions
        GraknCli initialOptionsPassCli = new GraknCli();
        CommandLine.populateCommand(new CommandLine(initialOptionsPassCli)
                .setUnmatchedArgumentsAllowed(true)
                .setUnmatchedOptionsArePositionalParams(true)
                .setOverwrittenOptionsAllowed(true), args);
        if (initialOptionsPassCli.showComponentErrors) {
            loader.addErrorListener(Throwable::printStackTrace);
        }

        CommandLine commandLine = new CommandLine(new GraknCli());

        // TODO Make this runtime configurable! We can load pretty much anything we like here.
        loader.load(new GraknComponentDefinition("server", "grakn.core.daemon.GraknDaemon", "io-grakn-core-grakn-daemon-0.0.0.jar"));
        loader.load(new GraknComponentDefinition("console", "grakn.console.GraknConsole", "io-grakn-console-grakn-console-0.0.0.jar"));

        loader.getComponents().forEach(component ->
                commandLine.addSubcommand(component.getCommand()));

        System.exit(commandLine
                .addSubcommand(Community.class)
                .addSubcommand(CommandLine.HelpCommand.class)
                .setColorScheme(COLOR_SCHEME)
                .setExecutionStrategy(parseResult -> {
                    if (parseResult.hasSubcommand()) {
                        GraknComponent component = loader.getComponent(parseResult.subcommand().commandSpec().name());
                        if (component != null) {
                            component.fullyLoad();
                        }
                    }
                    return new CommandLine.RunLast().execute(parseResult);
                }).execute(args));
    }
}
