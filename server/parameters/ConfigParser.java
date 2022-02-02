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

package com.vaticle.typedb.core.server.parameters;

import com.vaticle.typedb.common.yaml.Yaml;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.parameters.util.HelpEntry;
import com.vaticle.typedb.core.server.parameters.util.YamlParser;

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
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.Value.Primitive.BOOLEAN;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.Value.Primitive.BYTES_SIZE;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.Value.Primitive.INET_SOCKET_ADDRESS;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.Value.Primitive.LIST_STRING;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.Value.Primitive.PATH;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.Value.Primitive.STRING;
import static com.vaticle.typedb.core.server.common.Util.configPathAbsolute;
import static com.vaticle.typedb.core.server.common.Util.scopeKey;

public class ConfigParser extends YamlParser.Value.Compound<Config> {

    private static final YamlParser.KeyValue.Predefined<Config.Server> serverParser = YamlParser.KeyValue.Predefined.create(ServerParser.name, ServerParser.description, new ServerParser());
    private static final YamlParser.KeyValue.Predefined<Config.Storage> storageParser = YamlParser.KeyValue.Predefined.create(StorageParser.name, StorageParser.description, new StorageParser());
    private static final YamlParser.KeyValue.Predefined<Config.Log> logParser = YamlParser.KeyValue.Predefined.create(LogParser.name, LogParser.description, new LogParser());
    private static final YamlParser.KeyValue.Predefined<Config.VaticleFactory> vaticleFactoryParser = YamlParser.KeyValue.Predefined.create(VaticleFactoryParser.name, VaticleFactoryParser.description, new VaticleFactoryParser());
    private static final Set<YamlParser.KeyValue.Predefined<?>> entryParsers = set(serverParser, storageParser, logParser, vaticleFactoryParser);

    @Override
    public Config parse(Yaml yaml, String scope) {
        if (yaml.isMap()) {
            validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), "");
            return new Config(serverParser.parse(yaml.asMap(), ""), storageParser.parse(yaml.asMap(), ""),
                    logParser.parse(yaml.asMap(), ""), vaticleFactoryParser.parse(yaml.asMap(), "")
            );
        } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
    }

    @Override
    public List<HelpEntry> helpEntries(String scope) {
        return list(serverParser.helpEntry(scope), storageParser.helpEntry(scope), logParser.helpEntry(scope), vaticleFactoryParser.helpEntry(scope));
    }

    public static class ServerParser extends Compound<Config.Server> {

        private static final String name = "server";
        private static final String description = "Server and networking configuration.";

        private static final YamlParser.KeyValue.Predefined<InetSocketAddress> addressParser = YamlParser.KeyValue.Predefined.create("address", "Address to listen for GRPC clients on.", INET_SOCKET_ADDRESS);
        private static final Set<YamlParser.KeyValue.Predefined<?>> entryParsers = set(addressParser);

        @Override
        public Config.Server parse(Yaml yaml, String scope) {
            if (yaml.isMap()) {
                validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                return new Config.Server(addressParser.parse(yaml.asMap(), scope));
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
        }

        @Override
        public List<HelpEntry> helpEntries(String scope) {
            return list(addressParser.helpEntry(scope));
        }
    }

    static class StorageParser extends Compound<Config.Storage> {

        private static final String name = "storage";
        private static final String description = "Storage configuration.";

        private static final YamlParser.KeyValue.Predefined<Path> dataParser = YamlParser.KeyValue.Predefined.create("data", "Directory in which user databases will be stored.", PATH);
        private static final YamlParser.KeyValue.Predefined<Config.Storage.DatabaseCache> databaseCacheParser = YamlParser.KeyValue.Predefined.create(DatabaseCacheParser.name, DatabaseCacheParser.description, new DatabaseCacheParser());
        private static final Set<YamlParser.KeyValue.Predefined<?>> entryParsers = set(dataParser, databaseCacheParser);

        @Override
        public Config.Storage parse(Yaml yaml, String scope) {
            if (yaml.isMap()) {
                validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                return new Config.Storage(configPathAbsolute(dataParser.parse(yaml.asMap(), scope)),
                        databaseCacheParser.parse(yaml.asMap(), scope));
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
        }

        @Override
        public List<HelpEntry> helpEntries(String scope) {
            return list(dataParser.helpEntry(scope), databaseCacheParser.helpEntry(scope));
        }

        public static class DatabaseCacheParser extends Compound<Config.Storage.DatabaseCache> {

            public static final String name = "database-cache";
            public static final String description = "Per-database storage-layer cache configuration.";

            private static final YamlParser.KeyValue.Predefined<Long> dataParser = YamlParser.KeyValue.Predefined.create("data", "Size of storage-layer cache for data.", BYTES_SIZE);
            private static final YamlParser.KeyValue.Predefined<Long> indexParser = YamlParser.KeyValue.Predefined.create("index", "Size of storage-layer cache for index.", BYTES_SIZE);
            private static final Set<YamlParser.KeyValue.Predefined<?>> entryParsers = set(dataParser, indexParser);

            @Override
            public Config.Storage.DatabaseCache parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                    return new Config.Storage.DatabaseCache(dataParser.parse(yaml.asMap(), scope), indexParser.parse(yaml.asMap(), scope));
                } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
            }

            @Override
            public List<HelpEntry> helpEntries(String scope) {
                return list(dataParser.helpEntry(scope), indexParser.helpEntry(scope));
            }
        }
    }

    static class LogParser extends Compound<Config.Log> {

        public static final String name = "log";
        public static final String description = "Logging configuration.";

        private static final YamlParser.KeyValue.Predefined<Config.Log.Output> outputParser = YamlParser.KeyValue.Predefined.create(OutputParser.name, OutputParser.description, new OutputParser());
        private static final YamlParser.KeyValue.Predefined<Config.Log.Logger> loggerParser = YamlParser.KeyValue.Predefined.create(LoggerParser.name, LoggerParser.description, new LoggerParser());
        private static final YamlParser.KeyValue.Predefined<Config.Log.Debugger> debuggerParser = YamlParser.KeyValue.Predefined.create(DebuggerParser.name, DebuggerParser.description, new DebuggerParser());
        private static final Set<YamlParser.KeyValue.Predefined<?>> entryParsers = set(outputParser, loggerParser, debuggerParser);

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
        public List<HelpEntry> helpEntries(String scope) {
            return list(outputParser.helpEntry(scope), loggerParser.helpEntry(scope), debuggerParser.helpEntry(scope));
        }

        public static class OutputParser extends Compound<Config.Log.Output> {

            public static final String name = "output";
            public static final String description = "Log output definitions.";

            private static final YamlParser.KeyValue.Dynamic<Config.Log.Output.Type> typeEntry = YamlParser.KeyValue.Dynamic.create(TypeParser.description, new TypeParser());

            @Override
            public Config.Log.Output parse(Yaml yaml, String scope) {
                if (yaml.isMap()) return new Config.Log.Output(typeEntry.parseFrom(yaml.asMap(), scope));
                else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
            }

            @Override
            public List<HelpEntry> helpEntries(String scope) {
                return list(typeEntry.helpEntry(scope));
            }

            static class TypeParser extends Compound<Config.Log.Output.Type> {

                public final static String description = "A named log output definition.";
                private static final YamlParser.KeyValue.Predefined<String> typeParser = YamlParser.KeyValue.Predefined.create(
                        "type", "Type of output to define.", new Restricted<>(STRING, list(StdoutParser.type, FileParser.type))
                );
                private static final Compound<Config.Log.Output.Type.Stdout> stdoutParser = new StdoutParser();
                private static final Compound<Config.Log.Output.Type.File> fileParser = new FileParser();

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
                public List<HelpEntry> helpEntries(String scope) {
                    return list(new HelpEntry.Yaml.Grouped(scope, StdoutParser.description, stdoutParser.helpEntries(scope)),
                            new HelpEntry.Yaml.Grouped(scope, description, fileParser.helpEntries(scope)));
                }

                public static class StdoutParser extends Compound<Config.Log.Output.Type.Stdout> {

                    public static final String type = "stdout";
                    public static final String description = "Options to configure a log output to stdout.";

                    private static final YamlParser.KeyValue.Predefined<String> typeParser = YamlParser.KeyValue.Predefined.create(
                            "type", "An output that writes to stdout.", new Restricted<>(STRING, list(type))
                    );
                    private static final Set<YamlParser.KeyValue.Predefined<?>> entryParsers = set(typeParser);

                    @Override
                    public Config.Log.Output.Type.Stdout parse(Yaml yaml, String scope) {
                        if (yaml.isMap()) {
                            validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                            String type = typeParser.parse(yaml.asMap(), scope);
                            return new Config.Log.Output.Type.Stdout(type);
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                    }

                    @Override
                    public List<HelpEntry> helpEntries(String scope) {
                        return list(typeParser.helpEntry(scope));
                    }
                }

                public static class FileParser extends Compound<Config.Log.Output.Type.File> {

                    public static final String type = "file";
                    public static final String description = "Options to configure a log output to files in a directory.";

                    private static final YamlParser.KeyValue.Predefined<String> typeParser = YamlParser.KeyValue.Predefined.create("type", "An output that writes to a directory.", new Restricted<>(STRING, list(type)));
                    private static final YamlParser.KeyValue.Predefined<Path> pathParser = YamlParser.KeyValue.Predefined.create("directory", "Directory to write to. Relative paths are relative to distribution path.", PATH);
                    private static final YamlParser.KeyValue.Predefined<Long> fileSizeCapParser = YamlParser.KeyValue.Predefined.create("file-size-cap", "Log file size cap before creating new file (eg. 50mb).", BYTES_SIZE);
                    private static final YamlParser.KeyValue.Predefined<Long> archivesSizeCapParser = YamlParser.KeyValue.Predefined.create("archives-size-cap", "Total size cap of all archived log files in directory (eg. 1gb).", BYTES_SIZE); // TODO reasoner needs to respect this
                    private static final Set<YamlParser.KeyValue.Predefined<?>> entryParsers = set(typeParser, pathParser, fileSizeCapParser, archivesSizeCapParser);

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
                    public List<HelpEntry> helpEntries(String scope) {
                        return list(typeParser.helpEntry(scope), pathParser.helpEntry(scope), fileSizeCapParser.helpEntry(scope),
                                archivesSizeCapParser.helpEntry(scope));
                    }
                }
            }

        }

        public static class LoggerParser extends Compound<Config.Log.Logger> {

            private static final List<String> LEVELS = list("trace", "debug", "info", "warn", "error");

            public static final String name = "logger";
            public static final String description = "Loggers to activate.";

            private static final YamlParser.KeyValue.Predefined<Config.Log.Logger.Unfiltered> defaultParser = YamlParser.KeyValue.Predefined.create("default", "The default logger.", new UnfilteredParser());
            private static final YamlParser.KeyValue.Dynamic<Config.Log.Logger.Filtered> filteredParsers = YamlParser.KeyValue.Dynamic.create("Custom filtered loggers.", new FilteredParser());

            @Override
            public Config.Log.Logger parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    return new Config.Log.Logger(defaultParser.parse(yaml.asMap(), scope),
                            filteredParsers.parseFrom(yaml.asMap(), scope, defaultParser));
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
            }

            @Override
            public List<HelpEntry> helpEntries(String scope) {
                return list(defaultParser.helpEntry(scope), filteredParsers.helpEntry(scope));
            }

            static class UnfilteredParser extends Compound<Config.Log.Logger.Unfiltered> {

                private static final YamlParser.KeyValue.Predefined<String> levelParser = YamlParser.KeyValue.Predefined.create("level", "Output level.", new Restricted<>(STRING, LEVELS));
                private static final YamlParser.KeyValue.Predefined<List<String>> outputsParser = YamlParser.KeyValue.Predefined.create("output", "Outputs to log to by default.", LIST_STRING);
                private static final Set<YamlParser.KeyValue.Predefined<?>> entryParsers = set(levelParser, outputsParser);

                @Override
                public Config.Log.Logger.Unfiltered parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                        return new Config.Log.Logger.Unfiltered(levelParser.parse(yaml.asMap(), scope),
                                outputsParser.parse(yaml.asMap(), scope));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<HelpEntry> helpEntries(String scope) {
                    return list(levelParser.helpEntry(scope), outputsParser.helpEntry(scope));
                }
            }

            static class FilteredParser extends Compound<Config.Log.Logger.Filtered> {

                private static final YamlParser.KeyValue.Predefined<String> filterParser = YamlParser.KeyValue.Predefined.create("filter", "Package/class filter (eg. 'com.vaticle.typedb').", STRING);
                private static final YamlParser.KeyValue.Predefined<String> levelParser = YamlParser.KeyValue.Predefined.create("level", "Output level.", new Restricted<>(STRING, LEVELS));
                private static final YamlParser.KeyValue.Predefined<List<String>> outputsParser = YamlParser.KeyValue.Predefined.create("output", "Outputs to log to by default.", LIST_STRING);
                private static final Set<YamlParser.KeyValue.Predefined<?>> entryParsers = set(filterParser, levelParser, outputsParser);

                @Override
                public Config.Log.Logger.Filtered parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                        return new Config.Log.Logger.Filtered(levelParser.parse(yaml.asMap(), scope),
                                outputsParser.parse(yaml.asMap(), scope), filterParser.parse(yaml.asMap(), scope));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<HelpEntry> helpEntries(String scope) {
                    return list(filterParser.helpEntry(scope), levelParser.helpEntry(scope), outputsParser.helpEntry(scope));
                }
            }
        }

        public static class DebuggerParser extends Compound<Config.Log.Debugger> {

            public static final String name = "debugger";
            public static final String description = "Debuggers that may be enabled at runtime.";

            private static final YamlParser.KeyValue.Predefined<Config.Log.Debugger.Reasoner> reasonerParser = YamlParser.KeyValue.Predefined.create("reasoner", "Configure reasoner debugger.", new ReasonerParser());
            private static final Set<YamlParser.KeyValue.Predefined<?>> entryParsers = set(reasonerParser);

            @Override
            public Config.Log.Debugger parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                    return new Config.Log.Debugger(reasonerParser.parse(yaml.asMap(), scope));
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
            }

            @Override
            public List<HelpEntry> helpEntries(String scope) {
                return list(reasonerParser.helpEntry(scope));
            }

            static class ReasonerParser extends Compound<Config.Log.Debugger.Reasoner> {

                static final String type = "reasoner";
                private static final YamlParser.KeyValue.Predefined<String> typeParser = YamlParser.KeyValue.Predefined.create("type", "Type of this debugger.", new Restricted<>(STRING, list(type)));
                private static final YamlParser.KeyValue.Predefined<String> outputParser = YamlParser.KeyValue.Predefined.create("output", "Name of output reasoner debugger should write to (must be directory).", STRING);
                private static final YamlParser.KeyValue.Predefined<Boolean> enableParser = YamlParser.KeyValue.Predefined.create("enable", "Enable to allow reasoner debugging to be enabled at runtime.", BOOLEAN);
                private static final Set<YamlParser.KeyValue.Predefined<?>> entryParsers = set(typeParser, outputParser, enableParser);

                @Override
                public Config.Log.Debugger.Reasoner parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                        return new Config.Log.Debugger.Reasoner(typeParser.parse(yaml.asMap(), scope), outputParser.parse(yaml.asMap(), scope),
                                enableParser.parse(yaml.asMap(), scope));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<HelpEntry> helpEntries(String scope) {
                    return list(typeParser.helpEntry(scope), outputParser.helpEntry(scope), enableParser.helpEntry(scope));
                }
            }
        }
    }

    public static class VaticleFactoryParser extends Compound<Config.VaticleFactory> {

        public static final String name = "vaticle-factory";
        public static final String description = "Configure Vaticle Factory connection.";

        private static final YamlParser.KeyValue.Predefined<Boolean> enableParser = YamlParser.KeyValue.Predefined.create("enable", "Enable Vaticle Factory tracing.", BOOLEAN);
        private static final YamlParser.KeyValue.Predefined<String> uriParser = YamlParser.KeyValue.Predefined.create("uri", "URI of Vaticle Factory server.", STRING);
        private static final YamlParser.KeyValue.Predefined<String> usernameParser = YamlParser.KeyValue.Predefined.create("username", "Username for Vaticle Factory server.", STRING);
        private static final YamlParser.KeyValue.Predefined<String> tokenParser = YamlParser.KeyValue.Predefined.create("token", "Authentication token for Vaticle Factory server.", STRING);
        private static final Set<YamlParser.KeyValue.Predefined<?>> entryParsers = set(enableParser, uriParser, usernameParser, tokenParser);

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
        public List<HelpEntry> helpEntries(String scope) {
            return list(enableParser.helpEntry(scope), uriParser.helpEntry(scope), usernameParser.helpEntry(scope), tokenParser.helpEntry(scope));
        }
    }

    private static void validatedRecognisedParsers(Set<YamlParser.KeyValue.Predefined<?>> parsers, Set<String> keys, String scope) {
        Set<String> unrecognisedKeys = new HashSet<>(keys);
        parsers.forEach(parser -> unrecognisedKeys.remove(parser.key()));
        if (!unrecognisedKeys.isEmpty()) {
            Set<String> scopedKeys = iterate(unrecognisedKeys).map(key -> scopeKey(scope, key)).toSet();
            throw TypeDBException.of(UNRECOGNISED_CONFIGURATION_OPTIONS, scopedKeys);
        }
    }

}
