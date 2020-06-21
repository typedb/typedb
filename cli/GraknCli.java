package grakn.core.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.awt.Desktop;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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

    private static final CommandLine.Help.ColorScheme COLOR_SCHEME = new CommandLine.Help.ColorScheme.Builder()
            .commands    (CommandLine.Help.Ansi.Style.fg_white, CommandLine.Help.Ansi.Style.bold)
            .options     (CommandLine.Help.Ansi.Style.fg_cyan)
            .parameters  (CommandLine.Help.Ansi.Style.fg_yellow)
            .optionParams(CommandLine.Help.Ansi.Style.italic)
            .errors      (CommandLine.Help.Ansi.Style.fg_red, CommandLine.Help.Ansi.Style.bold)
            .stackTraces (CommandLine.Help.Ansi.Style.italic)
            .build();

    public static CommandLine buildCommand() {
        Path graknHome = Paths.get(Objects.requireNonNull(System.getProperty("grakn.dir")));

        List<ComponentSettings> graknComponents = Arrays.asList(
                new ComponentSettings("server", "grakn.core.daemon.GraknDaemon", "io-grakn-core-grakn-daemon-0.0.0.jar"),
                new ComponentSettings("console", "grakn.console.GraknConsole", "io-grakn-console-grakn-console-0.0.0.jar")
        );

        List<ComponentSettings> loaded = new ArrayList<>();

        graknComponents.forEach(component -> {
            try {
                component.load(graknHome);
                loaded.add(component);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        CommandLine commandLine = new CommandLine(new GraknCli());

        loaded.forEach(component -> {
            commandLine.addSubcommand(component.commandLine);
        });

        Map<String, ComponentSettings> componentSettingsMap = new HashMap<>();
        for (ComponentSettings component : loaded) {
            componentSettingsMap.put(component.componentName, component);
        }

        return commandLine
                .addSubcommand(Community.class)
                .addSubcommand(CommandLine.HelpCommand.class)
                .setColorScheme(COLOR_SCHEME)
                .setExecutionStrategy(parseResult -> {
                    if (parseResult.hasSubcommand()) {
                        ComponentSettings component = componentSettingsMap.get(parseResult.subcommand().commandSpec().name());
                        if (component != null) {
                            component.loader.addAllDeps();
                            componentSettingsMap.forEach((name, otherComponent) -> {
                                if (!name.equals(component.componentName)) {
                                    try {
                                        otherComponent.loader.close();
                                    } catch (Exception exception) {
                                        exception.printStackTrace();
                                    }
                                }
                            });
                        }
                    }
                    return new CommandLine.RunLast().execute(parseResult);
                });
    }

    public static void main(String[] args) {
        System.exit(buildCommand().execute(args));
    }

    private static class ComponentSettings {
        private String componentName;
        private String componentClass;
        private String componentJar;
        private GraknComponentLoader loader;
        private CommandLine commandLine;

        private ComponentSettings(String componentName, String componentClass, String componentJar) {
            this.componentName = componentName;
            this.componentClass = componentClass;
            this.componentJar = componentJar;
        }

        void load(Path graknHome) throws Exception {
            Path lib = graknHome.resolve(Paths.get(componentName, "services", "lib"));

            List<URL> libUrls;
            try {
                libUrls = Files.walk(lib)
                        .filter(Files::isRegularFile)
                        .map(Path::toUri)
                        .map(path -> {
                            try {
                                return path.toURL();
                            } catch (MalformedURLException e) {
                                throw new IllegalStateException();
                            }
                        })
                        .collect(Collectors.toList());
            } catch (IllegalStateException ex) {
                throw new Exception("Bad dependencies folder", ex);
            }

            libUrls.add(graknHome.resolve(Paths.get(componentName, "conf", "")).toUri().toURL());

            loader = new GraknComponentLoader(
                    componentClass,
                    new URL[]{graknHome.resolve(Paths.get(componentName, "services", "lib", componentJar)).toUri().toURL()},
                    libUrls.toArray(new URL[0])
            );

            commandLine = loader.loadCommand();
            commandLine.setCommandName(componentName);
        }
    }

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
}
