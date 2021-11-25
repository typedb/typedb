/*
 * Copyright (C) 2021 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.server.common;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.server.common.CommandLine.Option.CliHelp.Help;

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_FLAG_OPTION_HAS_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_REQUIRED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_REQUIRES_VALUE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_UNRECOGNISED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.DUPLICATE_CLI_ARGUMENT;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.server.common.Util.getConfigPath;
import static com.vaticle.typedb.core.server.common.Util.scopeKey;

public abstract class Command {

    private static final String COMMAND_PREFIX = "typedb server";

    public boolean isServer() {
        return false;
    }

    public Server asServer() {
        throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Server.class));
    }

    public boolean isImport() {
        return false;
    }

    public Import asImport() {
        throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Import.class));
    }

    public boolean isExport() {
        return false;
    }

    public Export asExport() {
        throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Export.class));
    }

    public static abstract class Parser<COMMAND extends Command> {

        private final String[] tokens;
        private final String description;

        Parser(String[] tokens, String description) {
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
            for (Help help : helpMenu()) {
                builder.append(help.toString());
            }
            return builder.toString();
        }

        abstract List<Help> helpMenu();
    }

    public static class Server extends Command {

        private static final String[] tokens = new String[]{};
        private static final String description = "Run TypeDB server";

        private final boolean isDebug;
        private final boolean isHelp;
        private final boolean isVersion;
        private final Configuration configuration;

        private Server(boolean isDebug, boolean isHelp, boolean isVersion, Configuration configuration) {
            this.isDebug = isDebug;
            this.isHelp = isHelp;
            this.isVersion = isVersion;
            this.configuration = configuration;
        }

        public static class Parser extends Command.Parser<Server> {

            private static final OptionParser.Flag debug = new OptionParser.Flag("debug", "Run server in debug mode.");
            private static final OptionParser.Flag help = new OptionParser.Flag("help", "Print help menu.");
            private static final OptionParser.Flag version = new OptionParser.Flag("version", "Print version number");
            private static final OptionParser.Path configFile = new OptionParser.Path("config", "Path to TypeDB YAML configuration file.");
            private static final Set<OptionParser> auxiliary = set(debug, help, version, configFile);

            private final Configuration.Parser configurationParser;

            public Parser(Configuration.Parser configurationParser) {
                super(Server.tokens, Server.description);
                this.configurationParser = configurationParser;
            }

            @Override
            public Server parse(Set<CommandLine.Option> cliOptions) {
                Set<CommandLine.Option> auxiliaryCliOptions = findAuxiliary(cliOptions);
                Set<CommandLine.Option> configurationOptions = excludeOptions(cliOptions, auxiliaryCliOptions);

                Configuration configuration = configFile.parse(auxiliaryCliOptions)
                        .map(file -> configurationParser.parse(getConfigPath(file), configurationOptions))
                        .orElseGet(() -> configurationParser.getDefault(configurationOptions));

                return new Server(debug.parse(auxiliaryCliOptions), help.parse(auxiliaryCliOptions),
                        version.parse(auxiliaryCliOptions), configuration);
            }

            private Set<CommandLine.Option> findAuxiliary(Set<CommandLine.Option> cliOptions) {
                return iterate(cliOptions).filter(option -> iterate(auxiliary).anyMatch(opt -> opt.name.equals(option.name()))).toSet();
            }

            private Set<CommandLine.Option> excludeOptions(Set<CommandLine.Option> options, Set<CommandLine.Option> exclude) {
                return iterate(options).filter(option -> !exclude.contains(option)).toSet();
            }


            @Override
            List<Help> helpMenu() {
                List<Help> options = list(help.help(), version.help(), debug.help(),
                        configFile.help());
                return list(options, configurationParser.help());
            }
        }

        @Override
        public boolean isServer() {
            return true;
        }

        @Override
        public Server asServer() {
            return this;
        }

        public boolean isDebug() {
            return isDebug;
        }

        public boolean isHelp() {
            return isHelp;
        }

        public boolean isVersion() {
            return isVersion;
        }

        public Configuration configuration() {
            return configuration;
        }
    }

    public static class Import extends Command {

        public static final String[] tokens = new String[]{"import"};
        public static final String description = "Run TypeDB import.";

        private final String database;
        private final String file;
        private final int typedbPort;

        Import(String database, String file, int typedbPort) {
            this.database = database;
            this.file = file;
            this.typedbPort = typedbPort;
        }

        public static class Parser extends Command.Parser<Import> {

            private static final OptionParser.String database = new OptionParser.String("database", "Database to import into.");
            private static final OptionParser.String dataFile = new OptionParser.String("file", "Path to data file to import (.typedb format).");
            private static final OptionParser.Int serverPort = new OptionParser.Int("port", "TypeDB's GRPC port.");
            private static final Set<OptionParser> auxiliary = set(database, dataFile, serverPort);

            public Parser() {
                super(Import.tokens, Import.description);
            }

            @Override
            public Import parse(Set<CommandLine.Option> options) {
                validateRequiredOptions(auxiliary, options);
                validateUnrecognisedOptions(auxiliary, options);
                return new Import(database.parse(options).get(), dataFile.parse(options).get(),
                        serverPort.parse(options).get());
            }

            @Override
            List<Help> helpMenu() {
                return list(database.help(), dataFile.help(), serverPort.help());
            }
        }

        public String database() {
            return database;
        }

        public String file() {
            return file;
        }

        public int typedbPort() {
            return typedbPort;
        }

        @Override
        public boolean isImport() {
            return true;
        }

        @Override
        public Import asImport() {
            return this;
        }
    }

    public static class Export extends Command {

        public static final String[] tokens = new String[]{"export"};
        public static final String description = "Run TypeDB export.";

        private final String database;
        private final String file;
        private final int typedbPort;

        public Export(String database, String file, int typedbPort) {
            this.database = database;
            this.file = file;
            this.typedbPort = typedbPort;
        }

        public static class Parser extends Command.Parser<Export> {

            private static final OptionParser.String database = new OptionParser.String("database", "Database to export.");
            private static final OptionParser.String dataFile = new OptionParser.String("file", "Path to data file to export to.");
            private static final OptionParser.Int typedbPort = new OptionParser.Int("port", "TypeDB's GRPC port.");
            private static final Set<OptionParser> auxiliary = set(database, dataFile, typedbPort);

            public Parser() {
                super(Export.tokens, Export.description);
            }

            @Override
            public Export parse(Set<CommandLine.Option> options) {
                validateRequiredOptions(auxiliary, options);
                validateUnrecognisedOptions(auxiliary, options);
                return new Export(database.parse(options).get(), dataFile.parse(options).get(),
                        typedbPort.parse(options).get());
            }

            @Override
            List<Help> helpMenu() {
                return list(database.help(), dataFile.help(), typedbPort.help());
            }
        }

        public String database() {
            return database;
        }

        public String file() {
            return file;
        }

        public int typedbPort() {
            return typedbPort;
        }

        @Override
        public boolean isExport() {
            return true;
        }

        @Override
        public Export asExport() {
            return this;
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
                if (matchingOptions(options).count() > 1) throw TypeDBException.of(DUPLICATE_CLI_ARGUMENT, name());
                Optional<CommandLine.Option> option = matchingOptions(options).first();
                if (option.isEmpty()) return false;
                else if (option.get().hasValue()) throw TypeDBException.of(CLI_FLAG_OPTION_HAS_VALUE, option.get());
                else return true;
            }
        }

        static class String extends OptionParser {

            private static final java.lang.String typeDescription = "string";

            String(java.lang.String name, java.lang.String description) {
                super(name, description, typeDescription);
            }

            Optional<java.lang.String> parse(Set<CommandLine.Option> options) {
                if (matchingOptions(options).count() > 1) throw TypeDBException.of(DUPLICATE_CLI_ARGUMENT, name());
                Optional<CommandLine.Option> option = matchingOptions(options).first();
                if (option.isEmpty()) return Optional.empty();
                else if (!option.get().hasValue()) {
                    throw TypeDBException.of(CLI_OPTION_REQUIRES_VALUE_TYPE, option.get(), typeDescription);
                } else {
                    return option.get().stringValue();
                }
            }
        }

        static class Path extends OptionParser {

            private static final java.lang.String typeDescription = "path";

            Path(java.lang.String name, java.lang.String description) {
                super(name, description, typeDescription);
            }

            Optional<java.nio.file.Path> parse(Set<CommandLine.Option> options) {
                if (matchingOptions(options).count() > 1) throw TypeDBException.of(DUPLICATE_CLI_ARGUMENT, name());
                Optional<CommandLine.Option> option = matchingOptions(options).first();
                if (option.isEmpty()) return Optional.empty();
                else if (!option.get().hasValue()) {
                    throw TypeDBException.of(CLI_OPTION_REQUIRES_VALUE_TYPE, option.get(), typeDescription);
                } else {
                    return option.get().stringValue().map(Paths::get);
                }
            }
        }

        static class Int extends OptionParser {

            private static final java.lang.String typeDescription = "int";

            Int(java.lang.String name, java.lang.String description) {
                super(name, description, typeDescription);
            }

            Optional<Integer> parse(Set<CommandLine.Option> options) {
                if (matchingOptions(options).count() > 1) throw TypeDBException.of(DUPLICATE_CLI_ARGUMENT, name());
                Optional<CommandLine.Option> option = matchingOptions(options).first();
                if (option.isEmpty()) return Optional.empty();
                else if (!option.get().hasValue())
                    throw TypeDBException.of(CLI_OPTION_REQUIRES_VALUE_TYPE, option.get());
                else {
                    try {
                        return Optional.of(Integer.parseInt(option.get().stringValue().get()));
                    } catch (NumberFormatException e) {
                        throw TypeDBException.of(CLI_OPTION_REQUIRES_VALUE_TYPE, option.get(), typeDescription);
                    }
                }
            }
        }
    }
}
