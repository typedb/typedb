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
 */

package com.vaticle.typedb.core.server.parameters.cli;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.common.parser.Describable;
import com.vaticle.typedb.core.server.common.parser.cli.Option;
import com.vaticle.typedb.core.server.common.parser.cli.OptionParser;
import com.vaticle.typedb.core.server.parameters.config.Config;
import com.vaticle.typedb.core.server.parameters.config.ConfigFactory;
import com.vaticle.typedb.core.server.parameters.config.ConfigParser;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_REQUIRED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_UNRECOGNISED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class SubcommandParser {

    private static void validateRequiredArgs(Set<OptionParser> requiredParsers, Set<Option> options) {
        requiredParsers.forEach(required -> {
            if (iterate(options).noneMatch(option -> option.name().equals(required.name()))) {
                throw TypeDBException.of(CLI_OPTION_REQUIRED, required.help());
            }
        });
    }

    private static void validateUnrecognisedArgs(Set<OptionParser> recognisedParsers, Set<Option> options) {
        recognisedParsers.forEach(parser -> {
            if (iterate(options).noneMatch(option -> option.name().equals(parser.name()))) {
                throw TypeDBException.of(CLI_OPTION_UNRECOGNISED, parser);
            }
        });
    }

    public static class ServerParser extends com.vaticle.typedb.core.server.common.parser.cli.SubcommandParser<Subcommand.Server> {

        private static final OptionParser.Flag debugParser = new OptionParser.Flag("debug", "Run server in debug mode.");
        private static final OptionParser.Flag helpParser = new OptionParser.Flag("help", "Print help menu.");
        private static final OptionParser.Flag versionParser = new OptionParser.Flag("version", "Print version number.");
        private static final OptionParser.Path configPathParser = new OptionParser.Path("config", "Path to TypeDB YAML configuration file.");
        private static final Set<OptionParser> auxiliaryParsers = set(debugParser, helpParser, versionParser, configPathParser);
        private static final String[] tokens = new String[]{};
        private static final String description = "Run TypeDB server";

        private final ConfigParser configParser;

        public ServerParser(ConfigParser configParser) {
            super(tokens, description);
            this.configParser = configParser;
        }

        @Override
        protected Subcommand.Server parse(Set<Option> options) {
            Set<Option> auxOptions = findAuxiliaryOptions(options);
            Set<Option> configOptions = excludeOptions(options, auxOptions);
            Optional<Path> configPath = configPathParser.parse(auxOptions);
            Config config = configPath
                    .map(path -> ConfigFactory.create(path, configOptions, configParser))
                    .orElse(ConfigFactory.create(configOptions, configParser));
            return new Subcommand.Server(
                    debugParser.parse(auxOptions),
                    helpParser.parse(auxOptions),
                    versionParser.parse(auxOptions),
                    config
            );
        }

        private Set<Option> findAuxiliaryOptions(Set<Option> options) {
            return iterate(options).filter(arg -> iterate(auxiliaryParsers).anyMatch(opt -> opt.name.equals(arg.name()))).toSet();
        }

        private Set<Option> excludeOptions(Set<Option> options, Set<Option> exclude) {
            return iterate(options).filter(option -> !exclude.contains(option)).toSet();
        }

        @Override
        public List<Describable.Description> helpMenu() {
            List<Describable.Description> aux = list(helpParser.help(), versionParser.help(), debugParser.help(), configPathParser.help());
            return list(aux, configParser.help());
        }
    }

    public static class ImportParser extends com.vaticle.typedb.core.server.common.parser.cli.SubcommandParser<Subcommand.Import> {

        public static final String[] tokens = new String[]{"import"};
        public static final String description = "Run TypeDB import.";

        private static final OptionParser.String databaseParser = new OptionParser.String("database", "Database to import into.");
        private static final OptionParser.Path filePathParser = new OptionParser.Path("file", "Path to data file to import (.typedb format).");
        private static final OptionParser.Int portParser = new OptionParser.Int("port", "TypeDB's GRPC port.");
        private static final Set<OptionParser> parsers = set(databaseParser, filePathParser, portParser);

        public ImportParser() {
            super(tokens, description);
        }

        @Override
        protected Subcommand.Import parse(Set<Option> options) {
            validateRequiredArgs(parsers, options);
            validateUnrecognisedArgs(parsers, options);
            return new Subcommand.Import(databaseParser.parse(options).get(), filePathParser.parse(options).get(),
                    portParser.parse(options).get());
        }

        @Override
        public List<Describable.Description> helpMenu() {
            return list(databaseParser.help(), filePathParser.help(), portParser.help());
        }
    }

    public static class ExportParser extends com.vaticle.typedb.core.server.common.parser.cli.SubcommandParser<Subcommand.Export> {

        public static final String[] tokens = new String[]{"export"};
        public static final String description = "Run TypeDB export.";

        private static final OptionParser.String databaseParser = new OptionParser.String("database", "Database to export.");
        private static final OptionParser.Path filePathParser = new OptionParser.Path("file", "Path to data file to export to.");
        private static final OptionParser.Int portParser = new OptionParser.Int("port", "TypeDB's GRPC port.");
        private static final Set<OptionParser> parsers = set(databaseParser, filePathParser, portParser);

        public ExportParser() {
            super(tokens, description);
        }

        @Override
        protected Subcommand.Export parse(Set<Option> options) {
            validateRequiredArgs(parsers, options);
            validateUnrecognisedArgs(parsers, options);
            return new Subcommand.Export(databaseParser.parse(options).get(), filePathParser.parse(options).get(),
                    portParser.parse(options).get());
        }

        @Override
        public List<Describable.Description> helpMenu() {
            return list(databaseParser.help(), filePathParser.help(), portParser.help());
        }
    }
}
