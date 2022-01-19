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

import com.vaticle.typedb.common.yaml.Yaml;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.common.CommandLine.Option.CliHelp.Help;
import com.vaticle.typedb.core.server.common.ConfigKVParser.EntryParser;
import com.vaticle.typedb.core.server.common.ConfigKVParser.EntryParser.EnumValue;
import com.vaticle.typedb.core.server.common.ConfigKVParser.EntryParser.Value;
import com.vaticle.typedb.core.server.common.ConfigKVParser.MapParser;
import com.vaticle.typedb.core.server.common.ConfigKVParser.ValueParser;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_REQUIRES_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_OUTPUT_UNRECOGNISED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_REASONER_REQUIRES_DIR_OUTPUT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_SECTION_MUST_BE_MAP;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_YAML_MUST_BE_MAP;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNRECOGNISED_CONFIGURATION_OPTIONS;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.server.common.ConfigKVParser.ValueParser.Leaf.BOOLEAN;
import static com.vaticle.typedb.core.server.common.ConfigKVParser.ValueParser.Leaf.BYTES_SIZE;
import static com.vaticle.typedb.core.server.common.ConfigKVParser.ValueParser.Leaf.INET_SOCKET_ADDRESS;
import static com.vaticle.typedb.core.server.common.ConfigKVParser.ValueParser.Leaf.LIST_STRING;
import static com.vaticle.typedb.core.server.common.ConfigKVParser.ValueParser.Leaf.PATH;
import static com.vaticle.typedb.core.server.common.ConfigKVParser.ValueParser.Leaf.STRING;
import static com.vaticle.typedb.core.server.common.Constants.CONFIG_PATH;
import static com.vaticle.typedb.core.server.common.Util.getConfigPath;
import static com.vaticle.typedb.core.server.common.Util.readConfig;
import static com.vaticle.typedb.core.server.common.Util.scopeKey;
import static com.vaticle.typedb.core.server.common.Util.setValue;

public class Configuration {

    protected final Server server;
    protected final Storage storage;
    protected final Log log;
    protected final VaticleFactory vaticleFactory;

    protected Configuration(Server server, Storage storage, Log log, @Nullable VaticleFactory vaticleFactory) {
        this.server = server;
        this.storage = storage;
        this.log = log;
        this.vaticleFactory = vaticleFactory;
    }

    public static class Parser {

        private static final EntryParser<Server> serverParser = Value.create(Server.name, Server.description, new Server.Parser());
        private static final EntryParser<Storage> storageParser = Value.create(Storage.name, Storage.description, new Storage.Parser());
        private static final EntryParser<Log> logParser = Value.create(Log.name, Log.description, new Log.Parser());
        private static final EntryParser<VaticleFactory> vaticleFactoryParser = Value.create(VaticleFactory.name, VaticleFactory.description, new VaticleFactory.Parser());
        private static final Set<EntryParser<?>> entryParsers = set(serverParser, storageParser, logParser, vaticleFactoryParser);

        public Configuration getConfig() {
            return getConfig(new HashSet<>());
        }

        public Configuration getConfig(Set<CommandLine.Option> overrides) {
            return getConfig(CONFIG_PATH, overrides);
        }

        public Configuration getConfig(Path configFile, Set<CommandLine.Option> overrides) {
            Yaml.Map yaml = readConfig(configFile);
            Map<String, Yaml> yamlOverrides = toYamlOverrides(overrides);
            yamlOverrides.forEach((key, value) -> setValue(yaml, key.split("\\."), value));
            validatedRecognisedParsers(entryParsers, yaml.keys(), "");
            return new Configuration(serverParser.parse(yaml, ""), storageParser.parse(yaml, ""),
                    logParser.parse(yaml, ""), vaticleFactoryParser.parse(yaml, "")
            );
        }

        private Map<String, Yaml> toYamlOverrides(Set<CommandLine.Option> options) {
            Set<String> keys = new HashSet<>();
            for (CommandLine.Option option : options) {
                if (!option.hasValue()) throw TypeDBException.of(CLI_OPTION_REQUIRES_VALUE, option);
                keys.add(option.name());
            }
            Map<String, Yaml> configOptions = new HashMap<>();
            for (String key : keys) {
                configOptions.put(key, Yaml.load(valueOrValueList(options, key)));
            }
            return configOptions;
        }

        private String valueOrValueList(Set<CommandLine.Option> options, String key) {
            Set<String> values = iterate(options).filter(opt -> opt.name().equals(key))
                    .map(opt -> opt.stringValue().get()).toSet();
            if (values.size() == 1) return values.iterator().next();
            else return "[" + String.join(", ", values) + "]";
        }

        public List<Help> help() {
            return list(serverParser.help(), storageParser.help(), logParser.help(), vaticleFactoryParser.help());
        }
    }

    public Server server() {
        return server;
    }

    public Storage storage() {
        return storage;
    }

    public Log log() {
        return log;
    }

    public VaticleFactory vaticleFactory() {
        return vaticleFactory;
    }

    public static class Server {

        private static final String name = "server";
        private static final String description = "Server and networking configuration.";

        private final InetSocketAddress address;

        protected Server(InetSocketAddress address) {
            this.address = address;
        }

        public static class Parser extends ValueParser.Nested<Server> {

            private static final EntryParser<InetSocketAddress> addressParser = Value.create("address", "Address to listen for GRPC clients on.", INET_SOCKET_ADDRESS);
            private static final Set<EntryParser<?>> entryParsers = set(addressParser);

            @Override
            public Server parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                    return new Server(addressParser.parse(yaml.asMap(), scope));
                } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
            }

            @Override
            public List<Help> help(String scope) {
                return list(addressParser.help(scope));
            }
        }

        public InetSocketAddress address() {
            return address;
        }
    }

    public static class Storage {

        private static final String name = "storage";
        private static final String description = "Storage configuration.";

        private final Path dataDir;
        private final DatabaseCache databaseCache;

        protected Storage(Path dataDir, DatabaseCache databaseCache) {
            this.dataDir = dataDir;
            this.databaseCache = databaseCache;
        }

        static class Parser extends ValueParser.Nested<Storage> {

            private static final EntryParser<Path> dataParser = Value.create("data", "Directory in which user databases will be stored.", PATH);
            private static final EntryParser<DatabaseCache> databaseCacheParser = Value.create(DatabaseCache.name, DatabaseCache.description, new DatabaseCache.Parser());
            private static final Set<EntryParser<?>> entryParsers = set(dataParser, databaseCacheParser);

            @Override
            public Storage parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                    return new Storage(getConfigPath(dataParser.parse(yaml.asMap(), scope)),
                            databaseCacheParser.parse(yaml.asMap(), scope));
                } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
            }

            @Override
            public List<Help> help(String scope) {
                return list(dataParser.help(scope), databaseCacheParser.help(scope));
            }
        }

        public Path dataDir() {
            return dataDir;
        }

        public DatabaseCache databaseCache() {
            return databaseCache;
        }

        public static class DatabaseCache {

            public static final String name = "database-cache";
            public static final String description = "Per-database storage-layer cache configuration.";

            private final long dataSize;
            private final long indexSize;

            private DatabaseCache(long dataSize, long indexSize) {
                this.dataSize = dataSize;
                this.indexSize = indexSize;
            }

            public static class Parser extends ValueParser.Nested<DatabaseCache> {

                private static final EntryParser<Long> dataParser = Value.create("data", "Size of storage-layer cache for data.", BYTES_SIZE);
                private static final EntryParser<Long> indexParser = Value.create("index", "Size of storage-layer cache for index.", BYTES_SIZE);
                private static final Set<EntryParser<?>> entryParsers = set(dataParser, indexParser);

                @Override
                public DatabaseCache parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                        return new DatabaseCache(dataParser.parse(yaml.asMap(), scope), indexParser.parse(yaml.asMap(), scope));
                    } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
                }

                @Override
                public List<Help> help(String scope) {
                    return list(dataParser.help(scope), indexParser.help(scope));
                }
            }

            public long dataSize() {
                return dataSize;
            }

            public long indexSize() {
                return indexSize;
            }
        }
    }

    public static class Log {

        public static final String name = "log";
        public static final String description = "Logging configuration.";

        private final Output output;
        private final Logger logger;
        private final Debugger debugger;

        public Log(Output output, Logger logger, Debugger debugger) {
            this.output = output;
            this.logger = logger;
            this.debugger = debugger;
        }

        static class Parser extends ValueParser.Nested<Log> {

            private static final EntryParser<Output> outputParser = Value.create(Output.name, Output.description, new Output.Parser());
            private static final EntryParser<Logger> loggerParser = Value.create(Logger.name, Logger.description, new Logger.Parser());
            private static final EntryParser<Debugger> debuggerParser = Value.create(Debugger.name, Debugger.description, new Debugger.Parser());
            private static final Set<EntryParser<?>> entryParsers = set(outputParser, loggerParser, debuggerParser);

            @Override
            public Log parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                    Output output = outputParser.parse(yaml.asMap(), scope);
                    Logger logger = loggerParser.parse(yaml.asMap(), scope);
                    logger.validateOutputs(output.outputs());
                    Debugger debugger = debuggerParser.parse(yaml.asMap(), scope);
                    debugger.validateAndSetOutputs(output.outputs());
                    return new Log(output, logger, debugger);
                } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
            }

            @Override
            public List<Help> help(String scope) {
                return list(outputParser.help(scope), loggerParser.help(scope), debuggerParser.help(scope));
            }
        }

        public Output output() {
            return output;
        }

        public Logger logger() {
            return logger;
        }

        public Debugger debugger() {
            return debugger;
        }

        public static class Output {

            public static final String name = "output";
            public static final String description = "Log output definitions.";

            private final Map<String, Type> outputs;

            public Output(Map<String, Type> outputs) {
                this.outputs = outputs;
            }

            public static class Parser extends ValueParser.Nested<Output> {

                private static final MapParser<Type> typeEntry = MapParser.create(Type.description, new Type.Parser());

                @Override
                public Output parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) return new Output(typeEntry.parseFrom(yaml.asMap(), scope));
                    else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<Help> help(String scope) {
                    return list(typeEntry.help(scope));
                }
            }

            public Map<String, Type> outputs() {
                return outputs;
            }

            public static abstract class Type {

                public final static String description = "A named log output definition.";

                public boolean isStdout() {
                    return false;
                }

                public Stdout asStdout() {
                    throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(Stdout.class));
                }

                public boolean isFile() {
                    return false;
                }

                public File asFile() {
                    throw TypeDBException.of(ILLEGAL_CAST, className(getClass()), className(File.class));
                }

                static class Parser extends ValueParser.Nested<Type> {

                    private static final EntryParser<String> typeParser = EnumValue.create("type", "Type of output to define.", STRING, list(Type.Stdout.type, File.type));
                    private static final ValueParser.Nested<Stdout> stdoutParser = new Stdout.Parser();
                    private static final ValueParser.Nested<File> fileParser = new File.Parser();

                    @Override
                    public Type parse(Yaml yaml, String scope) {
                        if (yaml.isMap()) {
                            String type = typeParser.parse(yaml.asMap(), scope);
                            switch (type) {
                                case Type.Stdout.type:
                                    return stdoutParser.parse(yaml.asMap(), scope);
                                case File.type:
                                    return fileParser.parse(yaml.asMap(), scope);
                                default:
                                    throw TypeDBException.of(ILLEGAL_STATE);
                            }
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                    }

                    @Override
                    public List<Help> help(String scope) {
                        return list(new Help.Section(scope, Stdout.description, stdoutParser.help(scope)),
                                new Help.Section(scope, File.description, fileParser.help(scope)));
                    }
                }

                public static class Stdout extends Type {

                    public static final String type = "stdout";
                    public static final String description = "Options to configure a log output to stdout.";

                    private Stdout(String type) {
                        assert type.equals(Stdout.type);
                    }

                    @Override
                    public boolean isStdout() {
                        return true;
                    }

                    @Override
                    public Stdout asStdout() {
                        return this;
                    }

                    public static class Parser extends ValueParser.Nested<Stdout> {

                        private static final EntryParser<String> typeParser = EnumValue.create("type", "An output that writes to stdout.", STRING, list(Stdout.type));
                        private static final Set<EntryParser<?>> entryParsers = set(typeParser);

                        @Override
                        public Stdout parse(Yaml yaml, String scope) {
                            if (yaml.isMap()) {
                                validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                                String type = typeParser.parse(yaml.asMap(), scope);
                                return new Stdout(type);
                            } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                        }

                        @Override
                        public List<Help> help(String scope) {
                            return list(typeParser.help(scope));
                        }
                    }
                }

                public static class File extends Type {

                    public static final String type = "file";
                    public static final String description = "Options to configure a log output to files in a directory.";

                    private final Path path;
                    private final long fileSizeCap;
                    private final long archivesSizeCap;

                    private File(String type, Path path, long fileSizeCap, long archivesSizeCap) {
                        assert type.equals(File.type);
                        this.path = path;
                        this.fileSizeCap = fileSizeCap;
                        this.archivesSizeCap = archivesSizeCap;
                    }

                    public static class Parser extends ValueParser.Nested<File> {

                        private static final EntryParser<String> typeParser = EnumValue.create("type", "An output that writes to a directory.", STRING, list(File.type));
                        private static final EntryParser<Path> pathParser = Value.create("directory", "Directory to write to. Relative paths are relative to distribution path.", PATH);
                        private static final EntryParser<Long> fileSizeCapParser = Value.create("file-size-cap", "Log file size cap before creating new file (eg. 50mb).", BYTES_SIZE);
                        private static final EntryParser<Long> archivesSizeCapParser = Value.create("archives-size-cap", "Total size cap of all archived log files in directory (eg. 1gb).", BYTES_SIZE); // TODO reasoner needs to respect this
                        private static final Set<EntryParser<?>> entryParsers = set(typeParser, pathParser, fileSizeCapParser, archivesSizeCapParser);

                        @Override
                        public File parse(Yaml yaml, String scope) {
                            if (yaml.isMap()) {
                                validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                                return new File(
                                        typeParser.parse(yaml.asMap(), scope),
                                        getConfigPath(pathParser.parse(yaml.asMap(), scope)),
                                        fileSizeCapParser.parse(yaml.asMap(), scope),
                                        archivesSizeCapParser.parse(yaml.asMap(), scope)
                                );
                            } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                        }

                        @Override
                        public List<Help> help(String scope) {
                            return list(typeParser.help(scope), pathParser.help(scope), fileSizeCapParser.help(scope),
                                    archivesSizeCapParser.help(scope));
                        }
                    }

                    public Path path() {
                        return path;
                    }

                    public long fileSizeCap() {
                        return fileSizeCap;
                    }

                    public long archivesSizeCap() {
                        return archivesSizeCap;
                    }

                    @Override
                    public boolean isFile() {
                        return true;
                    }

                    @Override
                    public File asFile() {
                        return this;
                    }
                }
            }
        }

        public static class Logger {
            private static final List<String> LEVELS = list("trace", "debug", "info", "warn", "error");

            public static final String name = "logger";
            public static final String description = "Loggers to activate.";

            private final Unfiltered defaultLogger;
            private final Map<String, Filtered> filteredLoggers;

            private Logger(Unfiltered defaultLogger, Map<String, Filtered> filteredLoggers) {
                this.defaultLogger = defaultLogger;
                this.filteredLoggers = filteredLoggers;
            }

            void validateOutputs(Map<String, Output.Type> outputs) {
                defaultLogger.validateOutputs(outputs);
                filteredLoggers.values().forEach(logger -> logger.validateOutputs(outputs));
            }

            public Unfiltered defaultLogger() {
                return defaultLogger;
            }

            public Map<String, Filtered> filteredLoggers() {
                return filteredLoggers;
            }

            public static class Parser extends ValueParser.Nested<Logger> {

                private static final EntryParser<Unfiltered> defaultParser = Value.create("default", "The default logger.", new Unfiltered.Parser());
                private static final MapParser<Filtered> filteredParsers = MapParser.create("Custom filtered loggers.", new Filtered.Parser());

                @Override
                public Logger parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        return new Logger(defaultParser.parse(yaml.asMap(), scope),
                                filteredParsers.parseFrom(yaml.asMap(), scope, defaultParser));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<Help> help(String scope) {
                    return list(defaultParser.help(scope), filteredParsers.help(scope));
                }
            }

            public static class Unfiltered {

                private final String level;
                private final List<String> outputNames;

                private Unfiltered(String level, List<String> outputNames) {
                    this.level = level;
                    this.outputNames = outputNames;
                }

                static class Parser extends ValueParser.Nested<Unfiltered> {

                    private static final EntryParser<String> levelParser = EnumValue.create("level", "Output level.", STRING, LEVELS);
                    private static final EntryParser<List<String>> outputsParser = Value.create("output", "Outputs to log to by default.", LIST_STRING);
                    private static final Set<EntryParser<?>> entryParsers = set(levelParser, outputsParser);

                    @Override
                    public Unfiltered parse(Yaml yaml, String scope) {
                        if (yaml.isMap()) {
                            validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                            return new Unfiltered(levelParser.parse(yaml.asMap(), scope),
                                    outputsParser.parse(yaml.asMap(), scope));
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                    }

                    @Override
                    public List<Help> help(String scope) {
                        return list(levelParser.help(scope), outputsParser.help(scope));
                    }
                }

                public String level() {
                    return level;
                }

                public List<String> outputs() {
                    return outputNames;
                }

                void validateOutputs(Map<String, Output.Type> outputsAvailable) {
                    outputNames.forEach(name -> {
                        if (!outputsAvailable.containsKey(name)) {
                            throw TypeDBException.of(CONFIG_OUTPUT_UNRECOGNISED, name);
                        }
                    });
                }
            }

            public static class Filtered extends Unfiltered {

                private final String filter;

                private Filtered(String level, List<String> outputNames, String filter) {
                    super(level, outputNames);
                    this.filter = filter;
                }

                public String filter() {
                    return filter;
                }

                static class Parser extends ValueParser.Nested<Filtered> {

                    private static final EntryParser<String> filterParser = Value.create("filter", "Class filter (eg. com.vaticle.type).", STRING);
                    private static final EntryParser<String> levelParser = EnumValue.create("level", "Output level.", STRING, LEVELS);
                    private static final EntryParser<List<String>> outputsParser = Value.create("output", "Outputs to log to by default.", LIST_STRING);
                    private static final Set<EntryParser<?>> entryParsers = set(filterParser, levelParser, outputsParser);

                    @Override
                    public Filtered parse(Yaml yaml, String scope) {
                        if (yaml.isMap()) {
                            validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                            return new Filtered(levelParser.parse(yaml.asMap(), scope),
                                    outputsParser.parse(yaml.asMap(), scope), filterParser.parse(yaml.asMap(), scope));
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                    }

                    @Override
                    public List<Help> help(String scope) {
                        return list(filterParser.help(scope), levelParser.help(scope), outputsParser.help(scope));
                    }
                }
            }
        }

        public static class Debugger {

            public static final String name = "debugger";
            public static final String description = "Debuggers that may be enabled at runtime.";

            private final Reasoner reasoner;

            private Debugger(Reasoner reasoner) {
                this.reasoner = reasoner;
            }

            public static class Parser extends ValueParser.Nested<Debugger> {

                private static final EntryParser<Reasoner> reasonerParser = Value.create("reasoner", "Configure reasoner debugger.", new Reasoner.Parser());
                private static final Set<EntryParser<?>> entryParsers = set(reasonerParser);

                @Override
                public Debugger parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                        return new Debugger(reasonerParser.parse(yaml.asMap(), scope));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<Help> help(String scope) {
                    return list(reasonerParser.help(scope));
                }
            }

            public Reasoner reasoner() {
                return reasoner;
            }

            void validateAndSetOutputs(Map<String, Output.Type> outputs) {
                reasoner.validateAndSetOutputs(outputs);
            }

            public static class Reasoner {

                private static final String type = "reasoner";

                private final String outputName;
                private final boolean enable;
                private Output.Type.File output;

                private Reasoner(String type, String outputName, boolean enable) {
                    assert type.equals(Reasoner.type);
                    this.outputName = outputName;
                    this.enable = enable;
                }

                public void validateAndSetOutputs(Map<String, Output.Type> outputs) {
                    assert output == null;
                    if (!outputs.containsKey(outputName))
                        throw TypeDBException.of(CONFIG_OUTPUT_UNRECOGNISED, outputName);
                    else if (!outputs.get(outputName).isFile()) {
                        throw TypeDBException.of(CONFIG_REASONER_REQUIRES_DIR_OUTPUT);
                    }
                    output = outputs.get(outputName).asFile();
                }

                public boolean isEnabled() {
                    return enable;
                }

                public Output.Type.File output() {
                    assert output != null;
                    return output;
                }

                static class Parser extends ValueParser.Nested<Reasoner> {

                    private static final EntryParser<String> typeParser = EnumValue.create("type", "Type of this debugger.", STRING, list(Reasoner.type));
                    private static final EntryParser<String> outputParser = Value.create("output", "Name of output reasoner debugger should write to (must be directory).", STRING);
                    private static final EntryParser<Boolean> enableParser = Value.create("enable", "Enable to allow reasoner debugging to be enabled at runtime.", BOOLEAN);
                    private static final Set<EntryParser<?>> entryParsers = set(typeParser, outputParser, enableParser);

                    @Override
                    public Reasoner parse(Yaml yaml, String scope) {
                        if (yaml.isMap()) {
                            validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                            return new Reasoner(typeParser.parse(yaml.asMap(), scope), outputParser.parse(yaml.asMap(), scope),
                                    enableParser.parse(yaml.asMap(), scope));
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                    }

                    @Override
                    public List<Help> help(String scope) {
                        return list(typeParser.help(scope), outputParser.help(scope), enableParser.help(scope));
                    }
                }
            }
        }
    }

    /**
     * Until the scope expands, we take this to only mean configuration of vaticle-factory tracing
     */
    public static class VaticleFactory {

        public static final String name = "vaticle-factory";
        public static final String description = "Configure Vaticle Factory connection.";

        private final boolean enable;
        private final String uri;
        private final String username;
        private final String token;

        private VaticleFactory(boolean enable, @Nullable String uri, @Nullable String username, @Nullable String token) {
            this.enable = enable;
            this.uri = uri;
            this.username = username;
            this.token = token;
        }

        public static class Parser extends ValueParser.Nested<VaticleFactory> {

            private static final EntryParser<Boolean> enableParser = Value.create("enable", "Enable Vaticle Factory tracing.", BOOLEAN);
            private static final EntryParser<String> uriParser = Value.create("uri", "URI of Vaticle Factory server.", STRING);
            private static final EntryParser<String> usernameParser = Value.create("username", "Username for Vaticle Factory server.", STRING);
            private static final EntryParser<String> tokenParser = Value.create("token", "Authentication token for Vaticle Factory server.", STRING);
            private static final Set<EntryParser<?>> entryParsers = set(enableParser, uriParser, usernameParser, tokenParser);

            @Override
            public VaticleFactory parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    boolean trace = enableParser.parse(yaml.asMap(), scope);
                    validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                    if (trace) {
                        return new VaticleFactory(true, uriParser.parse(yaml.asMap(), scope),
                                usernameParser.parse(yaml.asMap(), scope), tokenParser.parse(yaml.asMap(), scope));
                    } else {
                        return new VaticleFactory(false, null, null, null);
                    }
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
            }

            @Override
            public List<Help> help(String scope) {
                return list(enableParser.help(scope), uriParser.help(scope), usernameParser.help(scope), tokenParser.help(scope));
            }
        }

        public boolean enable() {
            return enable;
        }

        public Optional<String> uri() {
            return Optional.ofNullable(uri);
        }

        public Optional<String> username() {
            return Optional.ofNullable(username);
        }

        public Optional<String> token() {
            return Optional.ofNullable(token);
        }
    }

    private static void validatedRecognisedParsers(Set<EntryParser<?>> parsers, Set<String> keys, String scope) {
        Set<String> unrecognisedKeys = new HashSet<>(keys);
        parsers.forEach(parser -> unrecognisedKeys.remove(parser.key()));
        if (!unrecognisedKeys.isEmpty()) {
            Set<String> scopedKeys = iterate(unrecognisedKeys).map(key -> scopeKey(scope, key)).toSet();
            throw TypeDBException.of(UNRECOGNISED_CONFIGURATION_OPTIONS, scopedKeys);
        }
    }
}
