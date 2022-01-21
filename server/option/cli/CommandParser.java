package com.vaticle.typedb.core.server.option.cli;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.server.option.conf.Config;
import com.vaticle.typedb.core.server.option.conf.ConfigParser;

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_FLAG_OPTION_HAS_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_REQUIRED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_REQUIRES_TYPED_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_UNRECOGNISED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.DUPLICATE_CLI_OPTION;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.server.common.Util.getConfigPath;
import static com.vaticle.typedb.core.server.common.Util.scopeKey;

public abstract class CommandParser<COMMAND extends Command> {

    private static final String COMMAND_PREFIX = "typedb server";
    private final String[] tokens;
    private final String description;

    CommandParser(String[] tokens, String description) {
        this.tokens = tokens;
        this.description = description;
    }

    public abstract COMMAND parse(Set<CommandLine.Option> options);

    void validateRequiredOptions(Set<OptionParser> requiredParsers, Set<CommandLine.Option> options) {
        requiredParsers.forEach(required -> {
            if (iterate(options).noneMatch(option -> option.name().equals(required.name()))) {
                throw TypeDBException.of(CLI_OPTION_REQUIRED, required.help());
            }
        });
    }

    void validateUnrecognisedOptions(Set<OptionParser> recognisedParsers, Set<CommandLine.Option> options) {
        recognisedParsers.forEach(parser -> {
            if (iterate(options).noneMatch(option -> option.name().equals(parser.name()))) {
                throw TypeDBException.of(CLI_OPTION_UNRECOGNISED, parser);
            }
        });
    }

    public String[] tokens() {
        return tokens;
    }

    public String usage() {
        return COMMAND_PREFIX + " " + String.join(" ", tokens) + "\t\t" + description;
    }

    public String help() {
        StringBuilder builder = new StringBuilder(String.format("%-40s \t\t %s\n", "typedb server " +
                String.join(" ", tokens), description));
        for (CommandLine.Option.CliHelp.Help help : helpMenu()) {
            builder.append(help.toString());
        }
        return builder.toString();
    }

    abstract List<CommandLine.Option.CliHelp.Help> helpMenu();

    public static class ServerParser extends CommandParser<Command.Server> {

        private static final OptionParser.Flag debug = new OptionParser.Flag("debug", "Run server in debug mode.");
        private static final OptionParser.Flag help = new OptionParser.Flag("help", "Print help menu.");
        private static final OptionParser.Flag version = new OptionParser.Flag("version", "Print version number");
        private static final OptionParser.Path configFile = new OptionParser.Path("config", "Path to TypeDB YAML configuration file.");
        private static final Set<OptionParser> auxiliary = set(debug, help, version, configFile);
        private static final String[] tokens = new String[]{};
        private static final String description = "Run TypeDB server";

        private final ConfigParser configurationParser;

        public ServerParser(ConfigParser configurationParser) {
            super(tokens, description);
            this.configurationParser = configurationParser;
        }

        @Override
        public Command.Server parse(Set<CommandLine.Option> cliOptions) {
            Set<CommandLine.Option> auxiliaryCliOptions = findAuxiliary(cliOptions);
            Set<CommandLine.Option> configurationOptions = excludeOptions(cliOptions, auxiliaryCliOptions);

            Config config = configFile.parse(auxiliaryCliOptions)
                    .map(file -> configurationParser.getConfig(getConfigPath(file), configurationOptions))
                    .orElseGet(() -> configurationParser.getConfig(configurationOptions));

            return new Command.Server(debug.parse(auxiliaryCliOptions), help.parse(auxiliaryCliOptions),
                    version.parse(auxiliaryCliOptions), config);
        }

        private Set<CommandLine.Option> findAuxiliary(Set<CommandLine.Option> cliOptions) {
            return iterate(cliOptions).filter(option -> iterate(auxiliary).anyMatch(opt -> opt.name.equals(option.name()))).toSet();
        }

        private Set<CommandLine.Option> excludeOptions(Set<CommandLine.Option> options, Set<CommandLine.Option> exclude) {
            return iterate(options).filter(option -> !exclude.contains(option)).toSet();
        }


        @Override
        List<CommandLine.Option.CliHelp.Help> helpMenu() {
            List<CommandLine.Option.CliHelp.Help> options = list(help.help(), version.help(), debug.help(),
                    configFile.help());
            return list(options, configurationParser.help());
        }
    }

    public static class ImportParser extends CommandParser<Command.Import> {

        public static final String[] tokens = new String[]{"import"};
        public static final String description = "Run TypeDB import.";

        private static final OptionParser.String database = new OptionParser.String("database", "Database to import into.");
        private static final OptionParser.String dataFile = new OptionParser.String("file", "Path to data file to import (.typedb format).");
        private static final OptionParser.Int serverPort = new OptionParser.Int("port", "TypeDB's GRPC port.");
        private static final Set<OptionParser> auxiliary = set(database, dataFile, serverPort);

        public ImportParser() {
            super(tokens, description);
        }

        @Override
        public Command.Import parse(Set<CommandLine.Option> options) {
            validateRequiredOptions(auxiliary, options);
            validateUnrecognisedOptions(auxiliary, options);
            return new Command.Import(database.parse(options).get(), dataFile.parse(options).get(),
                    serverPort.parse(options).get());
        }

        @Override
        List<CommandLine.Option.CliHelp.Help> helpMenu() {
            return list(database.help(), dataFile.help(), serverPort.help());
        }
    }

    public static class ExportParser extends CommandParser<Command.Export> {

        public static final String[] tokens = new String[]{"export"};
        public static final String description = "Run TypeDB export.";

        private static final OptionParser.String database = new OptionParser.String("database", "Database to export.");
        private static final OptionParser.String dataFile = new OptionParser.String("file", "Path to data file to export to.");
        private static final OptionParser.Int typedbPort = new OptionParser.Int("port", "TypeDB's GRPC port.");
        private static final Set<OptionParser> auxiliary = set(database, dataFile, typedbPort);

        public ExportParser() {
            super(tokens, description);
        }

        @Override
        public Command.Export parse(Set<CommandLine.Option> options) {
            validateRequiredOptions(auxiliary, options);
            validateUnrecognisedOptions(auxiliary, options);
            return new Command.Export(database.parse(options).get(), dataFile.parse(options).get(),
                    typedbPort.parse(options).get());
        }

        @Override
        List<CommandLine.Option.CliHelp.Help> helpMenu() {
            return list(database.help(), dataFile.help(), typedbPort.help());
        }
    }

    abstract static class OptionParser implements CommandLine.Option.CliHelp {

        private final java.lang.String name;
        private final java.lang.String description;
        private final java.lang.String typeDescription;

        OptionParser(java.lang.String name, java.lang.String description, java.lang.String typeDescription) {
            this.name = name;
            this.description = description;
            this.typeDescription = typeDescription;
        }

        java.lang.String name() {
            return name;
        }

        java.lang.String description() {
            return description;
        }

        @Override
        public Help help(java.lang.String optionScope) {
            return new Help.Leaf(scopeKey(optionScope, name()), description(), typeDescription);
        }

        FunctionalIterator<CommandLine.Option> matchingOptions(Set<CommandLine.Option> options) {
            return iterate(options).filter(a -> a.name().equals(name()));
        }

        static class Flag extends OptionParser {

            Flag(java.lang.String name, java.lang.String description) {
                super(name, description, null);
            }

            boolean parse(Set<CommandLine.Option> options) {
                if (matchingOptions(options).count() > 1) throw TypeDBException.of(DUPLICATE_CLI_OPTION, name());
                Optional<CommandLine.Option> option = matchingOptions(options).first();
                if (option.isEmpty()) return false;
                else if (option.get().hasValue()) throw TypeDBException.of(CLI_FLAG_OPTION_HAS_VALUE, option.get());
                else return true;
            }
        }

        static class String extends OptionParser {

            private static final java.lang.String typeDescription = "<string>";

            String(java.lang.String name, java.lang.String description) {
                super(name, description, typeDescription);
            }

            Optional<java.lang.String> parse(Set<CommandLine.Option> options) {
                if (matchingOptions(options).count() > 1) throw TypeDBException.of(DUPLICATE_CLI_OPTION, name());
                Optional<CommandLine.Option> option = matchingOptions(options).first();
                if (option.isEmpty()) return Optional.empty();
                else if (!option.get().hasValue()) {
                    throw TypeDBException.of(CLI_OPTION_REQUIRES_TYPED_VALUE, option.get(), typeDescription);
                } else {
                    return option.get().stringValue();
                }
            }
        }

        static class Path extends OptionParser {

            private static final java.lang.String typeDescription = "<path>";

            Path(java.lang.String name, java.lang.String description) {
                super(name, description, typeDescription);
            }

            Optional<java.nio.file.Path> parse(Set<CommandLine.Option> options) {
                if (matchingOptions(options).count() > 1) throw TypeDBException.of(DUPLICATE_CLI_OPTION, name());
                Optional<CommandLine.Option> option = matchingOptions(options).first();
                if (option.isEmpty()) return Optional.empty();
                else if (!option.get().hasValue()) {
                    throw TypeDBException.of(CLI_OPTION_REQUIRES_TYPED_VALUE, option.get(), typeDescription);
                } else {
                    return option.get().stringValue().map(Paths::get);
                }
            }
        }

        static class Int extends OptionParser {

            private static final java.lang.String typeDescription = "<int>";

            Int(java.lang.String name, java.lang.String description) {
                super(name, description, typeDescription);
            }

            Optional<Integer> parse(Set<CommandLine.Option> options) {
                if (matchingOptions(options).count() > 1) throw TypeDBException.of(DUPLICATE_CLI_OPTION, name());
                Optional<CommandLine.Option> option = matchingOptions(options).first();
                if (option.isEmpty()) return Optional.empty();
                else if (!option.get().hasValue())
                    throw TypeDBException.of(CLI_OPTION_REQUIRES_TYPED_VALUE, option.get());
                else {
                    try {
                        return Optional.of(Integer.parseInt(option.get().stringValue().get()));
                    } catch (NumberFormatException e) {
                        throw TypeDBException.of(CLI_OPTION_REQUIRES_TYPED_VALUE, option.get(), typeDescription);
                    }
                }
            }
        }
    }
}
