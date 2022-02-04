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
import com.vaticle.typedb.core.server.common.Util;
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
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.KeyValue.Dynamic;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.KeyValue.Predefined;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.Value.Primitive.BOOLEAN;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.Value.Primitive.BYTES_SIZE;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.Value.Primitive.INET_SOCKET_ADDRESS;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.Value.Primitive.LIST_STRING;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.Value.Primitive.PATH;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.Value.Primitive.STRING;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.dynamic;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.predefined;
import static com.vaticle.typedb.core.server.parameters.util.YamlParser.restricted;

public class ConfigParser extends YamlParser.Value.Compound<Config> {

    private static final Predefined<Config.Server> server = predefined(Server.name, Server.description, new Server());
    private static final Predefined<Config.Storage> storage = predefined(Storage.name, Storage.description, new Storage());
    private static final Predefined<Config.Log> log = predefined(Log.name, Log.description, new Log());
    private static final Predefined<Config.VaticleFactory> vaticleFactory =
            predefined(VaticleFactory.name, VaticleFactory.description, new VaticleFactory());
    private static final Set<Predefined<?>> parsers = set(server, storage, log, vaticleFactory);

    @Override
    public Config parse(Yaml yaml, String path) {
        if (yaml.isMap()) {
            validatePredefinedKeys(parsers, yaml.asMap().keys(), "");
            return new Config(server.parse(yaml.asMap(), ""), storage.parse(yaml.asMap(), ""),
                    log.parse(yaml.asMap(), ""), vaticleFactory.parse(yaml.asMap(), "")
            );
        } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
    }

    @Override
    public List<HelpEntry> helpEntries(String path) {
        return list(server.helpEntry(path), storage.helpEntry(path), log.helpEntry(path), vaticleFactory.helpEntry(path));
    }

    private static Path configPathAbsolute(Path path) {
        if (path.isAbsolute()) return path;
        else return Util.getTypedbDir().resolve(path);
    }

    private static class Server extends Compound<Config.Server> {

        private static final String name = "server";
        private static final String description = "Server and networking configuration.";

        private static final Predefined<InetSocketAddress> address =
                predefined("address", "Address to listen for GRPC clients on.", INET_SOCKET_ADDRESS);
        private static final Set<Predefined<?>> parsers = set(address);

        @Override
        public Config.Server parse(Yaml yaml, String path) {
            if (yaml.isMap()) {
                validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                return new Config.Server(address.parse(yaml.asMap(), path));
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
        }

        @Override
        public List<HelpEntry> helpEntries(String path) {
            return list(address.helpEntry(path));
        }
    }

    static class Storage extends Compound<Config.Storage> {

        private static final String name = "storage";
        private static final String description = "Storage configuration.";

        private static final Predefined<Path> data =
                predefined("data", "Directory in which user databases will be stored.", PATH);
        private static final Predefined<Config.Storage.DatabaseCache> dbCache =
                predefined(DatabaseCache.name, DatabaseCache.description, new DatabaseCache());
        private static final Set<Predefined<?>> parsers = set(data, dbCache);

        @Override
        public Config.Storage parse(Yaml yaml, String path) {
            if (yaml.isMap()) {
                validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                return new Config.Storage(configPathAbsolute(data.parse(yaml.asMap(), path)),
                        dbCache.parse(yaml.asMap(), path));
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
        }

        @Override
        public List<HelpEntry> helpEntries(String path) {
            return list(data.helpEntry(path), dbCache.helpEntry(path));
        }

        private static class DatabaseCache extends Compound<Config.Storage.DatabaseCache> {

            private static final String name = "database-cache";
            private static final String description = "Per-database storage-layer cache configuration.";

            private static final Predefined<Long> data =
                    predefined("data", "Size of storage-layer cache for data.", BYTES_SIZE);
            private static final Predefined<Long> index =
                    predefined("index", "Size of storage-layer cache for index.", BYTES_SIZE);
            private static final Set<Predefined<?>> parsers = set(data, index);

            @Override
            public Config.Storage.DatabaseCache parse(Yaml yaml, String path) {
                if (yaml.isMap()) {
                    validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                    return new Config.Storage.DatabaseCache(data.parse(yaml.asMap(), path), index.parse(yaml.asMap(), path));
                } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
            }

            @Override
            public List<HelpEntry> helpEntries(String path) {
                return list(data.helpEntry(path), index.helpEntry(path));
            }
        }
    }

    static class Log extends Compound<Config.Log> {

        private static final String name = "log";
        private static final String description = "Logging configuration.";

        private static final Predefined<Config.Log.Output> output =
                predefined(Output.name, Output.description, new Output());
        private static final Predefined<Config.Log.Logger> logger =
                predefined(Logger.name, Logger.description, new Logger());
        private static final Predefined<Config.Log.Debugger> debugger =
                predefined(Debugger.name, Debugger.description, new Debugger());
        private static final Set<Predefined<?>> parsers = set(output, logger, debugger);

        @Override
        public Config.Log parse(Yaml yaml, String path) {
            if (yaml.isMap()) {
                validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                Config.Log.Output output = Log.output.parse(yaml.asMap(), path);
                Config.Log.Logger logger = Log.logger.parse(yaml.asMap(), path);
                logger.validateOutputs(output.outputs());
                Config.Log.Debugger debugger = Log.debugger.parse(yaml.asMap(), path);
                debugger.validateAndSetOutputs(output.outputs());
                return new Config.Log(output, logger, debugger);
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
        }

        @Override
        public List<HelpEntry> helpEntries(String path) {
            return list(output.helpEntry(path), logger.helpEntry(path), debugger.helpEntry(path));
        }

        private static class Output extends Compound<Config.Log.Output> {

            private static final String name = "output";
            private static final String description = "Log output definitions.";

            private static final Dynamic<Config.Log.Output.Type> type = dynamic(Type.description, new Type());

            @Override
            public Config.Log.Output parse(Yaml yaml, String path) {
                if (yaml.isMap()) return new Config.Log.Output(type.parseFrom(yaml.asMap(), path));
                else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
            }

            @Override
            public List<HelpEntry> helpEntries(String path) {
                return list(type.helpEntry(path));
            }

            static class Type extends Compound<Config.Log.Output.Type> {

                private static final String description = "A named log output definition.";
                private static final Predefined<String> type = predefined(
                        "type", "Type of output to define.", restricted(STRING, list(Stdout.type, File.type))
                );
                private static final Compound<Config.Log.Output.Type.Stdout> stdout = new Stdout();
                private static final Compound<Config.Log.Output.Type.File> file = new File();

                @Override
                public Config.Log.Output.Type parse(Yaml yaml, String path) {
                    if (yaml.isMap()) {
                        String value = type.parse(yaml.asMap(), path);
                        switch (value) {
                            case Stdout.type:
                                return stdout.parse(yaml.asMap(), path);
                            case File.type:
                                return file.parse(yaml.asMap(), path);
                            default:
                                throw TypeDBException.of(ILLEGAL_STATE);
                        }
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                }

                @Override
                public List<HelpEntry> helpEntries(String path) {
                    return list(new HelpEntry.Yaml.Grouped(path, Stdout.description, stdout.helpEntries(path)),
                            new HelpEntry.Yaml.Grouped(path, File.description, file.helpEntries(path)));
                }

                private static class Stdout extends Compound<Config.Log.Output.Type.Stdout> {

                    private static final String type = "stdout";
                    private static final String description = "Options to configure a log output to stdout.";

                    private static final Predefined<String> typeParser = predefined(
                            "type", "An output that writes to stdout.", restricted(STRING, list(type))
                    );
                    private static final Set<Predefined<?>> parsers = set(typeParser);

                    @Override
                    public Config.Log.Output.Type.Stdout parse(Yaml yaml, String path) {
                        if (yaml.isMap()) {
                            validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                            String type = typeParser.parse(yaml.asMap(), path);
                            assert Stdout.type.equals(type);
                            return new Config.Log.Output.Type.Stdout();
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                    }

                    @Override
                    public List<HelpEntry> helpEntries(String path) {
                        return list(typeParser.helpEntry(path));
                    }
                }

                private static class File extends Compound<Config.Log.Output.Type.File> {

                    private static final String type = "file";
                    private static final String description = "Options to configure a log output to files in a directory.";

                    private static final Predefined<String> typeParser =
                            predefined("type", "An output that writes to a directory.", restricted(STRING, list(type)));
                    private static final Predefined<Path> path =
                            predefined("directory", "Directory to write to. Relative paths are relative to distribution path.", PATH);
                    private static final Predefined<Long> fileSizeCap =
                            predefined("file-size-cap", "Log file size cap before creating new file (eg. 50mb).", BYTES_SIZE);
                    private static final Predefined<Long> archivesSizeCap =
                            predefined(
                                    "archives-size-cap",
                                    "Total size cap of all archived log files in directory (eg. 1gb).",
                                    BYTES_SIZE
                            ); // TODO reasoner needs to respect this
                    private static final Set<Predefined<?>> parsers = set(typeParser, path, fileSizeCap, archivesSizeCap);

                    @Override
                    public Config.Log.Output.Type.File parse(Yaml yaml, String path) {
                        if (yaml.isMap()) {
                            validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                            String type = typeParser.parse(yaml.asMap(), path);
                            assert File.type.equals(type);
                            return new Config.Log.Output.Type.File(
                                    configPathAbsolute(File.path.parse(yaml.asMap(), path)),
                                    fileSizeCap.parse(yaml.asMap(), path),
                                    archivesSizeCap.parse(yaml.asMap(), path)
                            );
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                    }

                    @Override
                    public List<HelpEntry> helpEntries(String path) {
                        return list(typeParser.helpEntry(path), File.path.helpEntry(path),
                                fileSizeCap.helpEntry(path), archivesSizeCap.helpEntry(path));
                    }
                }
            }
        }

        private static class Logger extends Compound<Config.Log.Logger> {

            private static final List<String> LEVELS = list("trace", "debug", "info", "warn", "error");

            private static final String name = "logger";
            private static final String description = "Loggers to activate.";

            private static final Predefined<Config.Log.Logger.Unfiltered> defaultLogger =
                    predefined("default", "The default logger.", new Unfiltered());
            private static final Dynamic<Config.Log.Logger.Filtered> filteredLogger =
                    dynamic("Custom filtered loggers.", new Filtered());

            @Override
            public Config.Log.Logger parse(Yaml yaml, String path) {
                if (yaml.isMap()) {
                    return new Config.Log.Logger(defaultLogger.parse(yaml.asMap(), path),
                            filteredLogger.parseFrom(yaml.asMap(), path, defaultLogger));
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
            }

            @Override
            public List<HelpEntry> helpEntries(String path) {
                return list(defaultLogger.helpEntry(path), filteredLogger.helpEntry(path));
            }

            private static class Unfiltered extends Compound<Config.Log.Logger.Unfiltered> {

                private static final Predefined<String> level =
                        predefined("level", "Output level.", restricted(STRING, LEVELS));
                private static final Predefined<List<String>> output =
                        predefined("output", "Outputs to log to by default.", LIST_STRING);
                private static final Set<Predefined<?>> parsers = set(level, output);

                @Override
                public Config.Log.Logger.Unfiltered parse(Yaml yaml, String path) {
                    if (yaml.isMap()) {
                        validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                        return new Config.Log.Logger.Unfiltered(level.parse(yaml.asMap(), path),
                                output.parse(yaml.asMap(), path));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                }

                @Override
                public List<HelpEntry> helpEntries(String path) {
                    return list(level.helpEntry(path), output.helpEntry(path));
                }
            }

            private static class Filtered extends Compound<Config.Log.Logger.Filtered> {

                private static final Predefined<String> filter =
                        predefined("filter", "Package/class filter (eg. 'com.vaticle.typedb').", STRING);
                private static final Predefined<String> level =
                        predefined("level", "Output level.", restricted(STRING, LEVELS));
                private static final Predefined<List<String>> output =
                        predefined("output", "Outputs to log to by default.", LIST_STRING);
                private static final Set<Predefined<?>> parsers = set(filter, level, output);

                @Override
                public Config.Log.Logger.Filtered parse(Yaml yaml, String path) {
                    if (yaml.isMap()) {
                        validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                        return new Config.Log.Logger.Filtered(level.parse(yaml.asMap(), path),
                                output.parse(yaml.asMap(), path), filter.parse(yaml.asMap(), path));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                }

                @Override
                public List<HelpEntry> helpEntries(String path) {
                    return list(filter.helpEntry(path), level.helpEntry(path), output.helpEntry(path));
                }
            }
        }

        private static class Debugger extends Compound<Config.Log.Debugger> {

            private static final String name = "debugger";
            private static final String description = "Debuggers that may be enabled at runtime.";

            private static final Predefined<Config.Log.Debugger.Reasoner> reasoner =
                    predefined("reasoner", "Configure reasoner debugger.", new Reasoner());
            private static final Set<Predefined<?>> parsers = set(reasoner);

            @Override
            public Config.Log.Debugger parse(Yaml yaml, String path) {
                if (yaml.isMap()) {
                    validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                    return new Config.Log.Debugger(reasoner.parse(yaml.asMap(), path));
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
            }

            @Override
            public List<HelpEntry> helpEntries(String path) {
                return list(reasoner.helpEntry(path));
            }

            private static class Reasoner extends Compound<Config.Log.Debugger.Reasoner> {

                private static final String type = "reasoner";
                private static final Predefined<String> typeParser =
                        predefined("type", "Type of this debugger.", restricted(STRING, list(type)));
                private static final Predefined<String> output =
                        predefined("output", "Name of output reasoner debugger should write to (must be directory).", STRING);
                private static final Predefined<Boolean> enable =
                        predefined("enable", "Enable to allow reasoner debugging to be enabled at runtime.", BOOLEAN);
                private static final Set<Predefined<?>> parsers = set(typeParser, output, enable);

                @Override
                public Config.Log.Debugger.Reasoner parse(Yaml yaml, String path) {
                    if (yaml.isMap()) {
                        validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                        String type = typeParser.parse(yaml.asMap(), path);
                        assert Reasoner.type.equals(type);
                        return new Config.Log.Debugger.Reasoner(
                                output.parse(yaml.asMap(), path), enable.parse(yaml.asMap(), path));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                }

                @Override
                public List<HelpEntry> helpEntries(String path) {
                    return list(typeParser.helpEntry(path), output.helpEntry(path), enable.helpEntry(path));
                }
            }
        }
    }

    private static class VaticleFactory extends Compound<Config.VaticleFactory> {

        private static final String name = "vaticle-factory";
        private static final String description = "Configure Vaticle Factory connection.";

        private static final Predefined<Boolean> enable =
                predefined("enable", "Enable Vaticle Factory tracing.", BOOLEAN);
        private static final Predefined<String> uri =
                predefined("uri", "URI of Vaticle Factory server.", STRING);
        private static final Predefined<String> username =
                predefined("username", "Username for Vaticle Factory server.", STRING);
        private static final Predefined<String> token =
                predefined("token", "Authentication token for Vaticle Factory server.", STRING);
        private static final Set<Predefined<?>> parsers = set(enable, uri, username, token);

        @Override
        public Config.VaticleFactory parse(Yaml yaml, String path) {
            if (yaml.isMap()) {
                boolean trace = enable.parse(yaml.asMap(), path);
                validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                if (trace) {
                    return new Config.VaticleFactory(true, uri.parse(yaml.asMap(), path),
                            username.parse(yaml.asMap(), path), token.parse(yaml.asMap(), path));
                } else {
                    return new Config.VaticleFactory(false, null, null, null);
                }
            } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
        }

        @Override
        public List<HelpEntry> helpEntries(String path) {
            return list(enable.helpEntry(path), uri.helpEntry(path), username.helpEntry(path), token.helpEntry(path));
        }
    }

    private static void validatePredefinedKeys(Set<Predefined<?>> predefinedParsers, Set<String> keys, String path) {
        Set<String> unrecognisedKeys = new HashSet<>(keys);
        predefinedParsers.forEach(parser -> unrecognisedKeys.remove(parser.key()));
        if (!unrecognisedKeys.isEmpty()) {
            Set<String> childPaths = iterate(unrecognisedKeys).map(key -> YamlParser.concatenate(path, key)).toSet();
            throw TypeDBException.of(UNRECOGNISED_CONFIGURATION_OPTIONS, childPaths);
        }
    }
}
