/*
 * Copyright (C) 2022 Vaticle
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
 */

package com.vaticle.typedb.core.server.parameters;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.parameters.util.Help;
import com.vaticle.typedb.core.server.parameters.util.Option;
import com.vaticle.typedb.core.server.parameters.util.OptionParser;
import com.vaticle.typedb.core.server.parameters.util.SubcommandParser;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_REQUIRED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_UNRECOGNISED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class CoreSubcommandParser {

    protected static void validateRequiredOptions(Set<OptionParser> requiredParsers, Set<Option> options) {
        requiredParsers.forEach(parser -> {
            if (iterate(options).noneMatch(option -> option.name().equals(parser.name()))) {
                throw TypeDBException.of(CLI_OPTION_REQUIRED, parser.name());
            }
        });
    }

    protected static void validateUnrecognisedOptions(Set<OptionParser> recognisedParsers, Set<Option> options) {
        options.forEach(option -> {
            if (iterate(recognisedParsers).noneMatch(parser -> option.name().equals(parser.name()))) {
                throw TypeDBException.of(CLI_OPTION_UNRECOGNISED, option.name());
            }
        });
    }

    public static class Server extends SubcommandParser<CoreSubcommand.Server> {

        public static final String[] tokens = new String[]{};
        public static final String description = "Run TypeDB server";

        public static final OptionParser.Flag debug = new OptionParser.Flag("debug", "Run server in debug mode.");
        public static final OptionParser.Flag help = new OptionParser.Flag("help", "Print help menu.");
        public static final OptionParser.Flag version = new OptionParser.Flag("version", "Print version number.");
        public static final OptionParser.Path configPath =
                new OptionParser.Path("config", "Path to TypeDB YAML configuration file.");
        public static final Set<OptionParser> auxiliaryParsers = set(debug, help, version, configPath);

        private final CoreConfigParser configParser;

        public Server(CoreConfigParser configParser) {
            super(tokens, description);
            this.configParser = configParser;
        }

        @Override
        protected CoreSubcommand.Server parse(Set<Option> options) {
            Set<Option> auxOptions = findAuxiliaryOptions(auxiliaryParsers, options);
            Set<Option> configOptions = excludeOptions(options, auxOptions);
            Optional<Path> configPath = Server.configPath.parse(auxOptions);
            CoreConfig config = configPath
                    .map(path -> CoreConfigFactory.config(path, configOptions, configParser))
                    .orElseGet(() -> CoreConfigFactory.config(configOptions, configParser));
            return new CoreSubcommand.Server(debug.parse(auxOptions), help.parse(auxOptions),
                    version.parse(auxOptions), config);
        }

        public static Set<Option> findAuxiliaryOptions(Set<OptionParser> auxiliaryParsers, Set<Option> options) {
            return iterate(options)
                    .filter(opt -> iterate(auxiliaryParsers).anyMatch(auxParser -> auxParser.name.equals(opt.name())))
                    .toSet();
        }

        public static Set<Option> excludeOptions(Set<Option> options, Set<Option> exclude) {
            return iterate(options).filter(opt -> !exclude.contains(opt)).toSet();
        }

        @Override
        public List<Help> helpList() {
            List<Help> aux = list(help.help(), version.help(), debug.help(), configPath.help());
            return list(aux, configParser.helpList(""));
        }
    }

    public static class Import extends SubcommandParser<CoreSubcommand.Import> {

        public static final String[] tokens = new String[]{"import"};
        public static final String description = "Run TypeDB import.";

        public static final OptionParser.String database =
                new OptionParser.String("database", "Database to import into.");
        public static final OptionParser.Path filePath =
                new OptionParser.Path("file", "Path to data file to import (.typedb format).");
        public static final OptionParser.Int port = new OptionParser.Int("port", "TypeDB's GRPC port.");
        private static final Set<OptionParser> parsers = set(database, filePath, port);

        public Import() {
            super(tokens, description);
        }

        @Override
        protected CoreSubcommand.Import parse(Set<Option> options) {
            validateRequiredOptions(parsers, options);
            validateUnrecognisedOptions(parsers, options);
            return new CoreSubcommand.Import(database.parse(options).get(), filePath.parse(options).get(),
                    port.parse(options).get());
        }

        @Override
        public List<Help> helpList() {
            return list(database.help(), filePath.help(), port.help());
        }
    }

    public static class Export extends SubcommandParser<CoreSubcommand.Export> {

        public static final String[] tokens = new String[]{"export"};
        public static final String description = "Run TypeDB export.";

        public static final OptionParser.String database =
                new OptionParser.String("database", "Database to export.");
        public static final OptionParser.Path filePath =
                new OptionParser.Path("file", "Path to data file to export to.");
        public static final OptionParser.Int port = new OptionParser.Int("port", "TypeDB's GRPC port.");
        private static final Set<OptionParser> parsers = set(database, filePath, port);

        public Export() {
            super(tokens, description);
        }

        @Override
        protected CoreSubcommand.Export parse(Set<Option> options) {
            validateRequiredOptions(parsers, options);
            validateUnrecognisedOptions(parsers, options);
            return new CoreSubcommand.Export(database.parse(options).get(), filePath.parse(options).get(),
                    port.parse(options).get());
        }

        @Override
        public List<Help> helpList() {
            return list(database.help(), filePath.help(), port.help());
        }
    }
}
