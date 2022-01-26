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

package com.vaticle.typedb.core.server.parameters.config;

import com.vaticle.typedb.common.yaml.Yaml;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.common.parser.Description;
import com.vaticle.typedb.core.server.common.parser.yml.YamlParser;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_SECTION_MUST_BE_MAP;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_YAML_MUST_BE_MAP;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNRECOGNISED_CONFIGURATION_OPTIONS;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.server.common.parser.yml.YamlParser.ValueParser.Leaf.BOOLEAN;
import static com.vaticle.typedb.core.server.common.parser.yml.YamlParser.ValueParser.Leaf.BYTES_SIZE;
import static com.vaticle.typedb.core.server.common.parser.yml.YamlParser.ValueParser.Leaf.INET_SOCKET_ADDRESS;
import static com.vaticle.typedb.core.server.common.parser.yml.YamlParser.ValueParser.Leaf.LIST_STRING;
import static com.vaticle.typedb.core.server.common.parser.yml.YamlParser.ValueParser.Leaf.PATH;
import static com.vaticle.typedb.core.server.common.parser.yml.YamlParser.ValueParser.Leaf.STRING;
import static com.vaticle.typedb.core.server.common.Util.configPathAbsolute;
import static com.vaticle.typedb.core.server.common.Util.scopeKey;

public class ConfigParser extends YamlParser.ValueParser.Nested<Config> {

    private static final YamlParser.EntryParser<Config.Server> serverParser = YamlParser.EntryParser.Value.create(ServerParser.name, ServerParser.description, new ServerParser());
    private static final YamlParser.EntryParser<Config.Storage> storageParser = YamlParser.EntryParser.Value.create(StorageParser.name, StorageParser.description, new StorageParser());
    private static final YamlParser.EntryParser<Config.Log> logParser = YamlParser.EntryParser.Value.create(LogParser.name, LogParser.description, new LogParser());
    private static final YamlParser.EntryParser<Config.VaticleFactory> vaticleFactoryParser = YamlParser.EntryParser.Value.create(VaticleFactoryParser.name, VaticleFactoryParser.description, new VaticleFactoryParser());
    private static final Set<YamlParser.EntryParser<?>> entryParsers = set(serverParser, storageParser, logParser, vaticleFactoryParser);

    @Override
    public Config parse(Yaml yaml, String scope) {
        if (yaml.isMap()) {
            validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), "");
            return new Config(serverParser.parse(yaml.asMap(), ""), storageParser.parse(yaml.asMap(), ""),
                    logParser.parse(yaml.asMap(), ""), vaticleFactoryParser.parse(yaml.asMap(), "")
            );
        } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
    }

    public List<Description> help() {
        return list(serverParser.getDescription(), storageParser.getDescription(), logParser.getDescription(), vaticleFactoryParser.getDescription());
    }

    @Override
    public List<Description> help(String scope) {
        return null;
    }

    public static class ServerParser extends YamlParser.ValueParser.Nested<Config.Server> {

        private static final String name = "server";
        private static final String description = "Server and networking configuration.";

        private static final YamlParser.EntryParser<InetSocketAddress> addressParser = YamlParser.EntryParser.Value.create("address", "Address to listen for GRPC clients on.", INET_SOCKET_ADDRESS);
        private static final Set<YamlParser.EntryParser<?>> entryParsers = set(addressParser);

        @Override
        public Config.Server parse(Yaml yaml, String scope) {
            if (yaml.isMap()) {
                validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                return new Config.Server(addressParser.parse(yaml.asMap(), scope));
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
        }

        @Override
        public List<Description> help(String scope) {
            return list(addressParser.getDescription(scope));
        }
    }

    static class StorageParser extends YamlParser.ValueParser.Nested<Config.Storage> {

        private static final String name = "storage";
        private static final String description = "Storage configuration.";

        private static final YamlParser.EntryParser<Path> dataParser = YamlParser.EntryParser.Value.create("data", "Directory in which user databases will be stored.", PATH);
        private static final YamlParser.EntryParser<Config.Storage.DatabaseCache> databaseCacheParser = YamlParser.EntryParser.Value.create(DatabaseCacheParser.name, DatabaseCacheParser.description, new DatabaseCacheParser());
        private static final Set<YamlParser.EntryParser<?>> entryParsers = set(dataParser, databaseCacheParser);

        @Override
        public Config.Storage parse(Yaml yaml, String scope) {
            if (yaml.isMap()) {
                validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                return new Config.Storage(configPathAbsolute(dataParser.parse(yaml.asMap(), scope)),
                        databaseCacheParser.parse(yaml.asMap(), scope));
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
        }

        @Override
        public List<Description> help(String scope) {
            return list(dataParser.getDescription(scope), databaseCacheParser.getDescription(scope));
        }

        public static class DatabaseCacheParser extends Nested<Config.Storage.DatabaseCache> {

            public static final String name = "database-cache";
            public static final String description = "Per-database storage-layer cache configuration.";

            private static final YamlParser.EntryParser<Long> dataParser = YamlParser.EntryParser.Value.create("data", "Size of storage-layer cache for data.", BYTES_SIZE);
            private static final YamlParser.EntryParser<Long> indexParser = YamlParser.EntryParser.Value.create("index", "Size of storage-layer cache for index.", BYTES_SIZE);
            private static final Set<YamlParser.EntryParser<?>> entryParsers = set(dataParser, indexParser);

            @Override
            public Config.Storage.DatabaseCache parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                    return new Config.Storage.DatabaseCache(dataParser.parse(yaml.asMap(), scope), indexParser.parse(yaml.asMap(), scope));
                } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
            }

            @Override
            public List<Description> help(String scope) {
                return list(dataParser.getDescription(scope), indexParser.getDescription(scope));
            }
        }
    }

    static class LogParser extends YamlParser.ValueParser.Nested<Config.Log> {

        public static final String name = "log";
        public static final String description = "Logging configuration.";

        private static final YamlParser.EntryParser<Config.Log.Output> outputParser = YamlParser.EntryParser.Value.create(OutputParser.name, OutputParser.description, new OutputParser());
        private static final YamlParser.EntryParser<Config.Log.Logger> loggerParser = YamlParser.EntryParser.Value.create(LoggerParser.name, LoggerParser.description, new LoggerParser());
        private static final YamlParser.EntryParser<Config.Log.Debugger> debuggerParser = YamlParser.EntryParser.Value.create(DebuggerParser.name, DebuggerParser.description, new DebuggerParser());
        private static final Set<YamlParser.EntryParser<?>> entryParsers = set(outputParser, loggerParser, debuggerParser);

        @Override
        public Config.Log parse(Yaml yaml, String scope) {
            if (yaml.isMap()) {
                validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                Config.Log.Output output = outputParser.parse(yaml.asMap(), scope);
                Config.Log.Logger logger = loggerParser.parse(yaml.asMap(), scope);
                logger.validateOutputs(output.outputs());
                Config.Log.Debugger debugger = debuggerParser.parse(yaml.asMap(), scope);
                debugger.validateAndSetOutputs(output.outputs());
                return new Config.Log(output, logger, debugger);
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
        }

        @Override
        public List<Description> help(String scope) {
            return list(outputParser.getDescription(scope), loggerParser.getDescription(scope), debuggerParser.getDescription(scope));
        }

        public static class OutputParser extends Nested<Config.Log.Output> {

            public static final String name = "output";
            public static final String description = "Log output definitions.";

            private static final YamlParser.MapParser<Config.Log.Output.Type> typeEntry = YamlParser.MapParser.create(TypeParser.description, new TypeParser());

            @Override
            public Config.Log.Output parse(Yaml yaml, String scope) {
                if (yaml.isMap()) return new Config.Log.Output(typeEntry.parseFrom(yaml.asMap(), scope));
                else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
            }

            @Override
            public List<Description> help(String scope) {
                return list(typeEntry.getDescription(scope));
            }

            static class TypeParser extends Nested<Config.Log.Output.Type> {

                public final static String description = "A named log output definition.";
                private static final YamlParser.EntryParser<String> typeParser = YamlParser.EntryParser.EnumValue.create("type", "Type of output to define.", STRING, list(StdoutParser.type, FileParser.type));
                private static final Nested<Config.Log.Output.Type.Stdout> stdoutParser = new StdoutParser();
                private static final Nested<Config.Log.Output.Type.File> fileParser = new FileParser();

                @Override
                public Config.Log.Output.Type parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        String type = typeParser.parse(yaml.asMap(), scope);
                        switch (type) {
                            case StdoutParser.type:
                                return stdoutParser.parse(yaml.asMap(), scope);
                            case FileParser.type:
                                return fileParser.parse(yaml.asMap(), scope);
                            default:
                                throw TypeDBException.of(ILLEGAL_STATE);
                        }
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<Description> help(String scope) {
                    return list(new Description.Compound(scope, StdoutParser.description, stdoutParser.help(scope)),
                            new Description.Compound(scope, description, fileParser.help(scope)));
                }

                public static class StdoutParser extends Nested<Config.Log.Output.Type.Stdout> {

                    public static final String type = "stdout";
                    public static final String description = "Options to configure a log output to stdout.";

                    private static final YamlParser.EntryParser<String> typeParser = YamlParser.EntryParser.EnumValue.create("type", "An output that writes to stdout.", STRING, list(type));
                    private static final Set<YamlParser.EntryParser<?>> entryParsers = set(typeParser);

                    @Override
                    public Config.Log.Output.Type.Stdout parse(Yaml yaml, String scope) {
                        if (yaml.isMap()) {
                            validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                            String type = typeParser.parse(yaml.asMap(), scope);
                            return new Config.Log.Output.Type.Stdout(type);
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                    }

                    @Override
                    public List<Description> help(String scope) {
                        return list(typeParser.getDescription(scope));
                    }
                }

                public static class FileParser extends Nested<Config.Log.Output.Type.File> {

                    public static final String type = "file";
                    public static final String description = "Options to configure a log output to files in a directory.";

                    private static final YamlParser.EntryParser<String> typeParser = YamlParser.EntryParser.EnumValue.create("type", "An output that writes to a directory.", STRING, list(type));
                    private static final YamlParser.EntryParser<Path> pathParser = YamlParser.EntryParser.Value.create("directory", "Directory to write to. Relative paths are relative to distribution path.", PATH);
                    private static final YamlParser.EntryParser<Long> fileSizeCapParser = YamlParser.EntryParser.Value.create("file-size-cap", "Log file size cap before creating new file (eg. 50mb).", BYTES_SIZE);
                    private static final YamlParser.EntryParser<Long> archivesSizeCapParser = YamlParser.EntryParser.Value.create("archives-size-cap", "Total size cap of all archived log files in directory (eg. 1gb).", BYTES_SIZE); // TODO reasoner needs to respect this
                    private static final Set<YamlParser.EntryParser<?>> entryParsers = set(typeParser, pathParser, fileSizeCapParser, archivesSizeCapParser);

                    @Override
                    public Config.Log.Output.Type.File parse(Yaml yaml, String scope) {
                        if (yaml.isMap()) {
                            validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                            return new Config.Log.Output.Type.File(
                                    typeParser.parse(yaml.asMap(), scope),
                                    configPathAbsolute(pathParser.parse(yaml.asMap(), scope)),
                                    fileSizeCapParser.parse(yaml.asMap(), scope),
                                    archivesSizeCapParser.parse(yaml.asMap(), scope)
                            );
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                    }

                    @Override
                    public List<Description> help(String scope) {
                        return list(typeParser.getDescription(scope), pathParser.getDescription(scope), fileSizeCapParser.getDescription(scope),
                                archivesSizeCapParser.getDescription(scope));
                    }
                }
            }

        }

        public static class LoggerParser extends Nested<Config.Log.Logger> {

            private static final List<String> LEVELS = list("trace", "debug", "info", "warn", "error");

            public static final String name = "logger";
            public static final String description = "Loggers to activate.";

            private static final YamlParser.EntryParser<Config.Log.Logger.Unfiltered> defaultParser = YamlParser.EntryParser.Value.create("default", "The default logger.", new UnfilteredParser());
            private static final YamlParser.MapParser<Config.Log.Logger.Filtered> filteredParsers = YamlParser.MapParser.create("Custom filtered loggers.", new FiltredParser());

            @Override
            public Config.Log.Logger parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    return new Config.Log.Logger(defaultParser.parse(yaml.asMap(), scope),
                            filteredParsers.parseFrom(yaml.asMap(), scope, defaultParser));
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
            }

            @Override
            public List<Description> help(String scope) {
                return list(defaultParser.getDescription(scope), filteredParsers.getDescription(scope));
            }

            static class UnfilteredParser extends Nested<Config.Log.Logger.Unfiltered> {

                private static final YamlParser.EntryParser<String> levelParser = YamlParser.EntryParser.EnumValue.create("level", "Output level.", STRING, LEVELS);
                private static final YamlParser.EntryParser<List<String>> outputsParser = YamlParser.EntryParser.Value.create("output", "Outputs to log to by default.", LIST_STRING);
                private static final Set<YamlParser.EntryParser<?>> entryParsers = set(levelParser, outputsParser);

                @Override
                public Config.Log.Logger.Unfiltered parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                        return new Config.Log.Logger.Unfiltered(levelParser.parse(yaml.asMap(), scope),
                                outputsParser.parse(yaml.asMap(), scope));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<Description> help(String scope) {
                    return list(levelParser.getDescription(scope), outputsParser.getDescription(scope));
                }
            }

            static class FiltredParser extends Nested<Config.Log.Logger.Filtered> {

                private static final YamlParser.EntryParser<String> filterParser = YamlParser.EntryParser.Value.create("filter", "Class filter (eg. com.vaticle.type).", STRING);
                private static final YamlParser.EntryParser<String> levelParser = YamlParser.EntryParser.EnumValue.create("level", "Output level.", STRING, LEVELS);
                private static final YamlParser.EntryParser<List<String>> outputsParser = YamlParser.EntryParser.Value.create("output", "Outputs to log to by default.", LIST_STRING);
                private static final Set<YamlParser.EntryParser<?>> entryParsers = set(filterParser, levelParser, outputsParser);

                @Override
                public Config.Log.Logger.Filtered parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                        return new Config.Log.Logger.Filtered(levelParser.parse(yaml.asMap(), scope),
                                outputsParser.parse(yaml.asMap(), scope), filterParser.parse(yaml.asMap(), scope));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<Description> help(String scope) {
                    return list(filterParser.getDescription(scope), levelParser.getDescription(scope), outputsParser.getDescription(scope));
                }
            }
        }

        public static class DebuggerParser extends Nested<Config.Log.Debugger> {

            public static final String name = "debugger";
            public static final String description = "Debuggers that may be enabled at runtime.";

            private static final YamlParser.EntryParser<Config.Log.Debugger.Reasoner> reasonerParser = YamlParser.EntryParser.Value.create("reasoner", "Configure reasoner debugger.", new ReasonerParser());
            private static final Set<YamlParser.EntryParser<?>> entryParsers = set(reasonerParser);

            @Override
            public Config.Log.Debugger parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                    return new Config.Log.Debugger(reasonerParser.parse(yaml.asMap(), scope));
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
            }

            @Override
            public List<Description> help(String scope) {
                return list(reasonerParser.getDescription(scope));
            }

            static class ReasonerParser extends Nested<Config.Log.Debugger.Reasoner> {

                static final String type = "reasoner";
                private static final YamlParser.EntryParser<String> typeParser = YamlParser.EntryParser.EnumValue.create("type", "Type of this debugger.", STRING, list(type));
                private static final YamlParser.EntryParser<String> outputParser = YamlParser.EntryParser.Value.create("output", "Name of output reasoner debugger should write to (must be directory).", STRING);
                private static final YamlParser.EntryParser<Boolean> enableParser = YamlParser.EntryParser.Value.create("enable", "Enable to allow reasoner debugging to be enabled at runtime.", BOOLEAN);
                private static final Set<YamlParser.EntryParser<?>> entryParsers = set(typeParser, outputParser, enableParser);

                @Override
                public Config.Log.Debugger.Reasoner parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                        return new Config.Log.Debugger.Reasoner(typeParser.parse(yaml.asMap(), scope), outputParser.parse(yaml.asMap(), scope),
                                enableParser.parse(yaml.asMap(), scope));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<Description> help(String scope) {
                    return list(typeParser.getDescription(scope), outputParser.getDescription(scope), enableParser.getDescription(scope));
                }
            }
        }
    }

    public static class VaticleFactoryParser extends YamlParser.ValueParser.Nested<Config.VaticleFactory> {

        public static final String name = "vaticle-factory";
        public static final String description = "Configure Vaticle Factory connection.";

        private static final YamlParser.EntryParser<Boolean> enableParser = YamlParser.EntryParser.Value.create("enable", "Enable Vaticle Factory tracing.", BOOLEAN);
        private static final YamlParser.EntryParser<String> uriParser = YamlParser.EntryParser.Value.create("uri", "URI of Vaticle Factory server.", STRING);
        private static final YamlParser.EntryParser<String> usernameParser = YamlParser.EntryParser.Value.create("username", "Username for Vaticle Factory server.", STRING);
        private static final YamlParser.EntryParser<String> tokenParser = YamlParser.EntryParser.Value.create("token", "Authentication token for Vaticle Factory server.", STRING);
        private static final Set<YamlParser.EntryParser<?>> entryParsers = set(enableParser, uriParser, usernameParser, tokenParser);

        @Override
        public Config.VaticleFactory parse(Yaml yaml, String scope) {
            if (yaml.isMap()) {
                boolean trace = enableParser.parse(yaml.asMap(), scope);
                validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                if (trace) {
                    return new Config.VaticleFactory(true, uriParser.parse(yaml.asMap(), scope),
                            usernameParser.parse(yaml.asMap(), scope), tokenParser.parse(yaml.asMap(), scope));
                } else {
                    return new Config.VaticleFactory(false, null, null, null);
                }
            } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
        }

        @Override
        public List<Description> help(String scope) {
            return list(enableParser.getDescription(scope), uriParser.getDescription(scope), usernameParser.getDescription(scope), tokenParser.getDescription(scope));
        }
    }

    private static void validatedRecognisedParsers(Set<YamlParser.EntryParser<?>> parsers, Set<String> keys, String scope) {
        Set<String> unrecognisedKeys = new HashSet<>(keys);
        parsers.forEach(parser -> unrecognisedKeys.remove(parser.key()));
        if (!unrecognisedKeys.isEmpty()) {
            Set<String> scopedKeys = iterate(unrecognisedKeys).map(key -> scopeKey(scope, key)).toSet();
            throw TypeDBException.of(UNRECOGNISED_CONFIGURATION_OPTIONS, scopedKeys);
        }
    }

}
