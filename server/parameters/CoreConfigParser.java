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

public class CoreConfigParser extends YamlParser.Value.Compound<CoreConfig> {

    private static final Predefined<CoreConfig.Server> server = predefined(Server.name, Server.description, new Server());
    private static final Predefined<CoreConfig.Storage> storage = predefined(Storage.name, Storage.description, new Storage());
    private static final Predefined<CoreConfig.Log> log = predefined(Log.name, Log.description, new Log());
    public static final Predefined<CoreConfig.VaticleFactory> vaticleFactory =
            predefined(VaticleFactory.name, VaticleFactory.description, new VaticleFactory());
    private static final Set<Predefined<?>> parsers = set(server, storage, log, vaticleFactory);

    @Override
    public CoreConfig parse(Yaml yaml, String path) {
        if (yaml.isMap()) {
            validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
            return new CoreConfig(server.parse(yaml.asMap(), path), storage.parse(yaml.asMap(), path),
                    log.parse(yaml.asMap(), path), vaticleFactory.parse(yaml.asMap(), path)
            );
        } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
    }

    @Override
    public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
        return list(server.help(path), storage.help(path), log.help(path), vaticleFactory.help(path));
    }

    public static Path configPathAbsolute(Path path) {
        if (path.isAbsolute()) return path;
        else return Util.getTypedbDir().resolve(path);
    }

    public static class Server extends Compound<CoreConfig.Server> {

        public static final String name = "server";
        public static final String description = "Server and networking configuration.";

        public static final Predefined<InetSocketAddress> address =
                predefined("address", "Address to listen for TypeDB Clients on.", INET_SOCKET_ADDRESS);
        private static final Set<Predefined<?>> parsers = set(address);

        @Override
        public CoreConfig.Server parse(Yaml yaml, String path) {
            if (yaml.isMap()) {
                validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                return new CoreConfig.Server(address.parse(yaml.asMap(), path));
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
        }

        @Override
        public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
            return list(address.help(path));
        }
    }

    public static class Storage extends Compound<CoreConfig.Storage> {

        public static final String name = "storage";
        public static final String description = "Storage configuration.";

        public static final Predefined<Path> data =
                predefined("data", "Directory in which user databases will be stored.", PATH);
        public static final Predefined<CoreConfig.Storage.DatabaseCache> dbCache =
                predefined(DatabaseCache.name, DatabaseCache.description, new DatabaseCache());
        private static final Set<Predefined<?>> parsers = set(data, dbCache);

        @Override
        public CoreConfig.Storage parse(Yaml yaml, String path) {
            if (yaml.isMap()) {
                validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                return new CoreConfig.Storage(configPathAbsolute(data.parse(yaml.asMap(), path)),
                        dbCache.parse(yaml.asMap(), path));
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
        }

        @Override
        public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
            return list(data.help(path), dbCache.help(path));
        }

        private static class DatabaseCache extends Compound<CoreConfig.Storage.DatabaseCache> {

            private static final String name = "database-cache";
            private static final String description = "Per-database storage-layer cache configuration.";

            private static final Predefined<Long> data =
                    predefined("data", "Size of storage-layer cache for data.", BYTES_SIZE);
            private static final Predefined<Long> index =
                    predefined("index", "Size of storage-layer cache for index.", BYTES_SIZE);
            private static final Set<Predefined<?>> parsers = set(data, index);

            @Override
            public CoreConfig.Storage.DatabaseCache parse(Yaml yaml, String path) {
                if (yaml.isMap()) {
                    validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                    return new CoreConfig.Storage.DatabaseCache(data.parse(yaml.asMap(), path), index.parse(yaml.asMap(), path));
                } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
            }

            @Override
            public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                return list(data.help(path), index.help(path));
            }
        }
    }

    public static class Log extends Compound<CoreConfig.Log> {

        public static final String name = "log";
        public static final String description = "Logging configuration.";

        private static final Predefined<CoreConfig.Log.Output> output =
                predefined(Output.name, Output.description, new Output());
        public static final Predefined<CoreConfig.Log.Logger> logger =
                predefined(Logger.name, Logger.description, new Logger());
        public static final Predefined<CoreConfig.Log.Debugger> debugger =
                predefined(Debugger.name, Debugger.description, new Debugger());
        private static final Set<Predefined<?>> parsers = set(output, logger, debugger);

        @Override
        public CoreConfig.Log parse(Yaml yaml, String path) {
            if (yaml.isMap()) {
                validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                CoreConfig.Log.Output output = Log.output.parse(yaml.asMap(), path);
                CoreConfig.Log.Logger logger = Log.logger.parse(yaml.asMap(), path);
                logger.validateOutputs(output.outputs());
                CoreConfig.Log.Debugger debugger = Log.debugger.parse(yaml.asMap(), path);
                debugger.validateAndSetOutputs(output.outputs());
                return new CoreConfig.Log(output, logger, debugger);
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
        }

        @Override
        public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
            return list(output.help(path), logger.help(path), debugger.help(path));
        }

        public static class Output extends Compound<CoreConfig.Log.Output> {

            public static final String name = "output";
            public static final String description = "Log output definitions.";

            private static final Dynamic<CoreConfig.Log.Output.Type> type = dynamic(Type.description, new Type());

            @Override
            public CoreConfig.Log.Output parse(Yaml yaml, String path) {
                if (yaml.isMap()) return new CoreConfig.Log.Output(type.parseFrom(yaml.asMap(), path));
                else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
            }

            @Override
            public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                return list(type.help(path));
            }

            public static class Type extends Compound<CoreConfig.Log.Output.Type> {

                private static final String description = "A named log output definition.";
                private static final Predefined<String> type = predefined(
                        "type", "Type of output to define.", restricted(STRING, list(Stdout.type, File.type))
                );
                public static final Compound<CoreConfig.Log.Output.Type.Stdout> stdout = new Stdout();
                public static final Compound<CoreConfig.Log.Output.Type.File> file = new File();

                @Override
                public CoreConfig.Log.Output.Type parse(Yaml yaml, String path) {
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
                public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                    return list(new Help(path, Stdout.description, stdout.helpList(path)),
                            new Help(path, File.description, file.helpList(path)));
                }

                public static class Stdout extends Compound<CoreConfig.Log.Output.Type.Stdout> {

                    public static final String type = "stdout";
                    public static final String description = "Options to configure a log output to stdout.";

                    private static final Predefined<String> typeParser = predefined(
                            "type", "An output that writes to stdout.", restricted(STRING, list(type))
                    );
                    private static final Set<Predefined<?>> parsers = set(typeParser);

                    @Override
                    public CoreConfig.Log.Output.Type.Stdout parse(Yaml yaml, String path) {
                        if (yaml.isMap()) {
                            validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                            String type = typeParser.parse(yaml.asMap(), path);
                            assert Stdout.type.equals(type);
                            return new CoreConfig.Log.Output.Type.Stdout();
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                    }

                    @Override
                    public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                        return list(typeParser.help(path));
                    }
                }

                public static class File extends Compound<CoreConfig.Log.Output.Type.File> {

                    public static final String type = "file";
                    public static final String description = "Options to configure a log output to files in a directory.";

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
                    public CoreConfig.Log.Output.Type.File parse(Yaml yaml, String path) {
                        if (yaml.isMap()) {
                            validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                            String type = typeParser.parse(yaml.asMap(), path);
                            assert File.type.equals(type);
                            return new CoreConfig.Log.Output.Type.File(
                                    configPathAbsolute(File.path.parse(yaml.asMap(), path)),
                                    fileSizeCap.parse(yaml.asMap(), path),
                                    archivesSizeCap.parse(yaml.asMap(), path)
                            );
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                    }

                    @Override
                    public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                        return list(typeParser.help(path), File.path.help(path),
                                fileSizeCap.help(path), archivesSizeCap.help(path));
                    }
                }
            }
        }

        private static class Logger extends Compound<CoreConfig.Log.Logger> {

            private static final List<String> LEVELS = list("trace", "debug", "info", "warn", "error");

            private static final String name = "logger";
            private static final String description = "Loggers to activate.";

            private static final Predefined<CoreConfig.Log.Logger.Unfiltered> defaultLogger =
                    predefined("default", "The default logger.", new Unfiltered());
            private static final Dynamic<CoreConfig.Log.Logger.Filtered> filteredLogger =
                    dynamic("Custom filtered loggers.", new Filtered());

            @Override
            public CoreConfig.Log.Logger parse(Yaml yaml, String path) {
                if (yaml.isMap()) {
                    return new CoreConfig.Log.Logger(defaultLogger.parse(yaml.asMap(), path),
                            filteredLogger.parseFrom(yaml.asMap(), path, defaultLogger));
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
            }

            @Override
            public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                return list(defaultLogger.help(path), filteredLogger.help(path));
            }

            private static class Unfiltered extends Compound<CoreConfig.Log.Logger.Unfiltered> {

                private static final Predefined<String> level =
                        predefined("level", "Output level.", restricted(STRING, LEVELS));
                private static final Predefined<List<String>> output =
                        predefined("output", "Outputs to log to by default.", LIST_STRING);
                private static final Set<Predefined<?>> parsers = set(level, output);

                @Override
                public CoreConfig.Log.Logger.Unfiltered parse(Yaml yaml, String path) {
                    if (yaml.isMap()) {
                        validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                        return new CoreConfig.Log.Logger.Unfiltered(level.parse(yaml.asMap(), path),
                                output.parse(yaml.asMap(), path));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                }

                @Override
                public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                    return list(level.help(path), output.help(path));
                }
            }

            private static class Filtered extends Compound<CoreConfig.Log.Logger.Filtered> {

                private static final Predefined<String> filter =
                        predefined("filter", "Package/class filter (eg. 'com.vaticle.typedb').", STRING);
                private static final Predefined<String> level =
                        predefined("level", "Output level.", restricted(STRING, LEVELS));
                private static final Predefined<List<String>> output =
                        predefined("output", "Outputs to log to by default.", LIST_STRING);
                private static final Set<Predefined<?>> parsers = set(filter, level, output);

                @Override
                public CoreConfig.Log.Logger.Filtered parse(Yaml yaml, String path) {
                    if (yaml.isMap()) {
                        validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                        return new CoreConfig.Log.Logger.Filtered(level.parse(yaml.asMap(), path),
                                output.parse(yaml.asMap(), path), filter.parse(yaml.asMap(), path));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                }

                @Override
                public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                    return list(filter.help(path), level.help(path), output.help(path));
                }
            }
        }

        private static class Debugger extends Compound<CoreConfig.Log.Debugger> {

            private static final String name = "debugger";
            private static final String description = "Debuggers that may be enabled at runtime.";

            private static final Predefined<CoreConfig.Log.Debugger.Reasoner> reasoner =
                    predefined("reasoner", "Configure reasoner debugger.", new Reasoner());
            private static final Set<Predefined<?>> parsers = set(reasoner);

            @Override
            public CoreConfig.Log.Debugger parse(Yaml yaml, String path) {
                if (yaml.isMap()) {
                    validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                    return new CoreConfig.Log.Debugger(reasoner.parse(yaml.asMap(), path));
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
            }

            @Override
            public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                return list(reasoner.help(path));
            }

            private static class Reasoner extends Compound<CoreConfig.Log.Debugger.Reasoner> {

                private static final String type = "reasoner";
                private static final Predefined<String> typeParser =
                        predefined("type", "Type of this debugger.", restricted(STRING, list(type)));
                private static final Predefined<String> output =
                        predefined("output", "Name of output reasoner debugger should write to (must be directory).", STRING);
                private static final Predefined<Boolean> enable =
                        predefined("enable", "Enable to allow reasoner debugging to be enabled at runtime.", BOOLEAN);
                private static final Set<Predefined<?>> parsers = set(typeParser, output, enable);

                @Override
                public CoreConfig.Log.Debugger.Reasoner parse(Yaml yaml, String path) {
                    if (yaml.isMap()) {
                        validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                        String type = typeParser.parse(yaml.asMap(), path);
                        assert Reasoner.type.equals(type);
                        return new CoreConfig.Log.Debugger.Reasoner(
                                output.parse(yaml.asMap(), path), enable.parse(yaml.asMap(), path));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                }

                @Override
                public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                    return list(typeParser.help(path), output.help(path), enable.help(path));
                }
            }
        }
    }

    public static class VaticleFactory extends Compound<CoreConfig.VaticleFactory> {

        public static final String name = "vaticle-factory";
        public static final String description = "Configure Vaticle Factory connection.";

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
        public CoreConfig.VaticleFactory parse(Yaml yaml, String path) {
            if (yaml.isMap()) {
                boolean trace = enable.parse(yaml.asMap(), path);
                validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                if (trace) {
                    return new CoreConfig.VaticleFactory(true, uri.parse(yaml.asMap(), path),
                            username.parse(yaml.asMap(), path), token.parse(yaml.asMap(), path));
                } else {
                    return new CoreConfig.VaticleFactory(false, null, null, null);
                }
            } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
        }

        @Override
        public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
            return list(enable.help(path), uri.help(path), username.help(path), token.help(path));
        }
    }

    public static void validatePredefinedKeys(Set<Predefined<?>> predefinedParsers, Set<String> keys, String path) {
        Set<String> unrecognisedKeys = new HashSet<>(keys);
        predefinedParsers.forEach(parser -> unrecognisedKeys.remove(parser.key()));
        if (!unrecognisedKeys.isEmpty()) {
            Set<String> childPaths = iterate(unrecognisedKeys).map(key -> YamlParser.concatenate(path, key)).toSet();
            throw TypeDBException.of(UNRECOGNISED_CONFIGURATION_OPTIONS, childPaths);
        }
    }
}
