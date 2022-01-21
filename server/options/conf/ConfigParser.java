package com.vaticle.typedb.core.server.options.conf;

import com.vaticle.typedb.common.yaml.Yaml;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.options.cli.CommandLine;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CLI_OPTION_REQUIRES_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_SECTION_MUST_BE_MAP;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_YAML_MUST_BE_MAP;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNRECOGNISED_CONFIGURATION_OPTIONS;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.server.options.conf.ConfigKVParser.ValueParser.Leaf.BOOLEAN;
import static com.vaticle.typedb.core.server.options.conf.ConfigKVParser.ValueParser.Leaf.BYTES_SIZE;
import static com.vaticle.typedb.core.server.options.conf.ConfigKVParser.ValueParser.Leaf.INET_SOCKET_ADDRESS;
import static com.vaticle.typedb.core.server.options.conf.ConfigKVParser.ValueParser.Leaf.LIST_STRING;
import static com.vaticle.typedb.core.server.options.conf.ConfigKVParser.ValueParser.Leaf.PATH;
import static com.vaticle.typedb.core.server.options.conf.ConfigKVParser.ValueParser.Leaf.STRING;
import static com.vaticle.typedb.core.server.common.Constants.CONFIG_PATH;
import static com.vaticle.typedb.core.server.common.Util.getConfigPath;
import static com.vaticle.typedb.core.server.common.Util.readConfig;
import static com.vaticle.typedb.core.server.common.Util.scopeKey;
import static com.vaticle.typedb.core.server.common.Util.setValue;

public class ConfigParser {

    private static final ConfigKVParser.EntryParser<Config.Server> serverParser = ConfigKVParser.EntryParser.Value.create(ServerParser.name, ServerParser.description, new ServerParser());
    private static final ConfigKVParser.EntryParser<Config.Storage> storageParser = ConfigKVParser.EntryParser.Value.create(StorageParser.name, StorageParser.description, new StorageParser());
    private static final ConfigKVParser.EntryParser<Config.Log> logParser = ConfigKVParser.EntryParser.Value.create(LogParser.name, LogParser.description, new LogParser());
    private static final ConfigKVParser.EntryParser<Config.VaticleFactory> vaticleFactoryParser = ConfigKVParser.EntryParser.Value.create(VaticleFactoryParser.name, VaticleFactoryParser.description, new VaticleFactoryParser());
    private static final Set<ConfigKVParser.EntryParser<?>> entryParsers = set(serverParser, storageParser, logParser, vaticleFactoryParser);

    public Config getConfig() {
        return getConfig(new HashSet<>());
    }

    public Config getConfig(Set<CommandLine.Option> overrides) {
        return getConfig(CONFIG_PATH, overrides);
    }

    public Config getConfig(Path configFile, Set<CommandLine.Option> overrides) {
        Yaml.Map yaml = readConfig(configFile);
        Map<String, Yaml> yamlOverrides = toYamlOverrides(overrides);
        yamlOverrides.forEach((key, value) -> setValue(yaml, key.split("\\."), value));
        validatedRecognisedParsers(entryParsers, yaml.keys(), "");
        return new Config(serverParser.parse(yaml, ""), storageParser.parse(yaml, ""),
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

    public List<CommandLine.Option.CliHelp.Help> help() {
        return list(serverParser.help(), storageParser.help(), logParser.help(), vaticleFactoryParser.help());
    }

    public static class ServerParser extends ConfigKVParser.ValueParser.Nested<Config.Server> {

        private static final String name = "server";
        private static final String description = "Server and networking configuration.";

        private static final ConfigKVParser.EntryParser<InetSocketAddress> addressParser = ConfigKVParser.EntryParser.Value.create("address", "Address to listen for GRPC clients on.", INET_SOCKET_ADDRESS);
        private static final Set<ConfigKVParser.EntryParser<?>> entryParsers = set(addressParser);

        @Override
        public Config.Server parse(Yaml yaml, String scope) {
            if (yaml.isMap()) {
                validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                return new Config.Server(addressParser.parse(yaml.asMap(), scope));
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
        }

        @Override
        public List<CommandLine.Option.CliHelp.Help> help(String scope) {
            return list(addressParser.help(scope));
        }
    }

    static class StorageParser extends ConfigKVParser.ValueParser.Nested<Config.Storage> {

        private static final String name = "storage";
        private static final String description = "Storage configuration.";

        private static final ConfigKVParser.EntryParser<Path> dataParser = ConfigKVParser.EntryParser.Value.create("data", "Directory in which user databases will be stored.", PATH);
        private static final ConfigKVParser.EntryParser<Config.Storage.DatabaseCache> databaseCacheParser = ConfigKVParser.EntryParser.Value.create(DatabaseCacheParser.name, DatabaseCacheParser.description, new DatabaseCacheParser());
        private static final Set<ConfigKVParser.EntryParser<?>> entryParsers = set(dataParser, databaseCacheParser);

        @Override
        public Config.Storage parse(Yaml yaml, String scope) {
            if (yaml.isMap()) {
                validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                return new Config.Storage(getConfigPath(dataParser.parse(yaml.asMap(), scope)),
                        databaseCacheParser.parse(yaml.asMap(), scope));
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
        }

        @Override
        public List<CommandLine.Option.CliHelp.Help> help(String scope) {
            return list(dataParser.help(scope), databaseCacheParser.help(scope));
        }

        public static class DatabaseCacheParser extends Nested<Config.Storage.DatabaseCache> {

            public static final String name = "database-cache";
            public static final String description = "Per-database storage-layer cache configuration.";

            private static final ConfigKVParser.EntryParser<Long> dataParser = ConfigKVParser.EntryParser.Value.create("data", "Size of storage-layer cache for data.", BYTES_SIZE);
            private static final ConfigKVParser.EntryParser<Long> indexParser = ConfigKVParser.EntryParser.Value.create("index", "Size of storage-layer cache for index.", BYTES_SIZE);
            private static final Set<ConfigKVParser.EntryParser<?>> entryParsers = set(dataParser, indexParser);

            @Override
            public Config.Storage.DatabaseCache parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                    return new Config.Storage.DatabaseCache(dataParser.parse(yaml.asMap(), scope), indexParser.parse(yaml.asMap(), scope));
                } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, scope);
            }

            @Override
            public List<CommandLine.Option.CliHelp.Help> help(String scope) {
                return list(dataParser.help(scope), indexParser.help(scope));
            }
        }
    }

    static class LogParser extends ConfigKVParser.ValueParser.Nested<Config.Log> {

        public static final String name = "log";
        public static final String description = "Logging configuration.";

        private static final ConfigKVParser.EntryParser<Config.Log.Output> outputParser = ConfigKVParser.EntryParser.Value.create(OutputParser.name, OutputParser.description, new OutputParser());
        private static final ConfigKVParser.EntryParser<Config.Log.Logger> loggerParser = ConfigKVParser.EntryParser.Value.create(LoggerParser.name, LoggerParser.description, new LoggerParser());
        private static final ConfigKVParser.EntryParser<Config.Log.Debugger> debuggerParser = ConfigKVParser.EntryParser.Value.create(DebuggerParser.name, DebuggerParser.description, new DebuggerParser());
        private static final Set<ConfigKVParser.EntryParser<?>> entryParsers = set(outputParser, loggerParser, debuggerParser);

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
        public List<CommandLine.Option.CliHelp.Help> help(String scope) {
            return list(outputParser.help(scope), loggerParser.help(scope), debuggerParser.help(scope));
        }

        public static class OutputParser extends Nested<Config.Log.Output> {

            public static final String name = "output";
            public static final String description = "Log output definitions.";

            private static final ConfigKVParser.MapParser<Config.Log.Output.Type> typeEntry = ConfigKVParser.MapParser.create(TypeParser.description, new TypeParser());

            @Override
            public Config.Log.Output parse(Yaml yaml, String scope) {
                if (yaml.isMap()) return new Config.Log.Output(typeEntry.parseFrom(yaml.asMap(), scope));
                else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
            }

            @Override
            public List<CommandLine.Option.CliHelp.Help> help(String scope) {
                return list(typeEntry.help(scope));
            }

            static class TypeParser extends Nested<Config.Log.Output.Type> {

                public final static String description = "A named log output definition.";
                private static final ConfigKVParser.EntryParser<String> typeParser = ConfigKVParser.EntryParser.EnumValue.create("type", "Type of output to define.", STRING, list(StdoutParser.type, FileParser.type));
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
                public List<CommandLine.Option.CliHelp.Help> help(String scope) {
                    return list(new CommandLine.Option.CliHelp.Help.Section(scope, StdoutParser.description, stdoutParser.help(scope)),
                            new CommandLine.Option.CliHelp.Help.Section(scope, description, fileParser.help(scope)));
                }

                public static class StdoutParser extends Nested<Config.Log.Output.Type.Stdout> {

                    public static final String type = "stdout";
                    public static final String description = "Options to configure a log output to stdout.";

                    private static final ConfigKVParser.EntryParser<String> typeParser = ConfigKVParser.EntryParser.EnumValue.create("type", "An output that writes to stdout.", STRING, list(type));
                    private static final Set<ConfigKVParser.EntryParser<?>> entryParsers = set(typeParser);

                    @Override
                    public Config.Log.Output.Type.Stdout parse(Yaml yaml, String scope) {
                        if (yaml.isMap()) {
                            validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                            String type = typeParser.parse(yaml.asMap(), scope);
                            return new Config.Log.Output.Type.Stdout(type);
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                    }

                    @Override
                    public List<CommandLine.Option.CliHelp.Help> help(String scope) {
                        return list(typeParser.help(scope));
                    }
                }

                public static class FileParser extends Nested<Config.Log.Output.Type.File> {

                    public static final String type = "file";
                    public static final String description = "Options to configure a log output to files in a directory.";

                    private static final ConfigKVParser.EntryParser<String> typeParser = ConfigKVParser.EntryParser.EnumValue.create("type", "An output that writes to a directory.", STRING, list(type));
                    private static final ConfigKVParser.EntryParser<Path> pathParser = ConfigKVParser.EntryParser.Value.create("directory", "Directory to write to. Relative paths are relative to distribution path.", PATH);
                    private static final ConfigKVParser.EntryParser<Long> fileSizeCapParser = ConfigKVParser.EntryParser.Value.create("file-size-cap", "Log file size cap before creating new file (eg. 50mb).", BYTES_SIZE);
                    private static final ConfigKVParser.EntryParser<Long> archivesSizeCapParser = ConfigKVParser.EntryParser.Value.create("archives-size-cap", "Total size cap of all archived log files in directory (eg. 1gb).", BYTES_SIZE); // TODO reasoner needs to respect this
                    private static final Set<ConfigKVParser.EntryParser<?>> entryParsers = set(typeParser, pathParser, fileSizeCapParser, archivesSizeCapParser);

                    @Override
                    public Config.Log.Output.Type.File parse(Yaml yaml, String scope) {
                        if (yaml.isMap()) {
                            validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                            return new Config.Log.Output.Type.File(
                                    typeParser.parse(yaml.asMap(), scope),
                                    getConfigPath(pathParser.parse(yaml.asMap(), scope)),
                                    fileSizeCapParser.parse(yaml.asMap(), scope),
                                    archivesSizeCapParser.parse(yaml.asMap(), scope)
                            );
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                    }

                    @Override
                    public List<CommandLine.Option.CliHelp.Help> help(String scope) {
                        return list(typeParser.help(scope), pathParser.help(scope), fileSizeCapParser.help(scope),
                                archivesSizeCapParser.help(scope));
                    }
                }
            }

        }

        public static class LoggerParser extends Nested<Config.Log.Logger> {

            private static final List<String> LEVELS = list("trace", "debug", "info", "warn", "error");

            public static final String name = "logger";
            public static final String description = "Loggers to activate.";

            private static final ConfigKVParser.EntryParser<Config.Log.Logger.Unfiltered> defaultParser = ConfigKVParser.EntryParser.Value.create("default", "The default logger.", new UnfilteredParser());
            private static final ConfigKVParser.MapParser<Config.Log.Logger.Filtered> filteredParsers = ConfigKVParser.MapParser.create("Custom filtered loggers.", new FiltredParser());

            @Override
            public Config.Log.Logger parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    return new Config.Log.Logger(defaultParser.parse(yaml.asMap(), scope),
                            filteredParsers.parseFrom(yaml.asMap(), scope, defaultParser));
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
            }

            @Override
            public List<CommandLine.Option.CliHelp.Help> help(String scope) {
                return list(defaultParser.help(scope), filteredParsers.help(scope));
            }

            static class UnfilteredParser extends Nested<Config.Log.Logger.Unfiltered> {

                private static final ConfigKVParser.EntryParser<String> levelParser = ConfigKVParser.EntryParser.EnumValue.create("level", "Output level.", STRING, LEVELS);
                private static final ConfigKVParser.EntryParser<List<String>> outputsParser = ConfigKVParser.EntryParser.Value.create("output", "Outputs to log to by default.", LIST_STRING);
                private static final Set<ConfigKVParser.EntryParser<?>> entryParsers = set(levelParser, outputsParser);

                @Override
                public Config.Log.Logger.Unfiltered parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                        return new Config.Log.Logger.Unfiltered(levelParser.parse(yaml.asMap(), scope),
                                outputsParser.parse(yaml.asMap(), scope));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<CommandLine.Option.CliHelp.Help> help(String scope) {
                    return list(levelParser.help(scope), outputsParser.help(scope));
                }
            }

            static class FiltredParser extends Nested<Config.Log.Logger.Filtered> {

                private static final ConfigKVParser.EntryParser<String> filterParser = ConfigKVParser.EntryParser.Value.create("filter", "Class filter (eg. com.vaticle.type).", STRING);
                private static final ConfigKVParser.EntryParser<String> levelParser = ConfigKVParser.EntryParser.EnumValue.create("level", "Output level.", STRING, LEVELS);
                private static final ConfigKVParser.EntryParser<List<String>> outputsParser = ConfigKVParser.EntryParser.Value.create("output", "Outputs to log to by default.", LIST_STRING);
                private static final Set<ConfigKVParser.EntryParser<?>> entryParsers = set(filterParser, levelParser, outputsParser);

                @Override
                public Config.Log.Logger.Filtered parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                        return new Config.Log.Logger.Filtered(levelParser.parse(yaml.asMap(), scope),
                                outputsParser.parse(yaml.asMap(), scope), filterParser.parse(yaml.asMap(), scope));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<CommandLine.Option.CliHelp.Help> help(String scope) {
                    return list(filterParser.help(scope), levelParser.help(scope), outputsParser.help(scope));
                }
            }
        }

        public static class DebuggerParser extends Nested<Config.Log.Debugger> {

            public static final String name = "debugger";
            public static final String description = "Debuggers that may be enabled at runtime.";

            private static final ConfigKVParser.EntryParser<Config.Log.Debugger.Reasoner> reasonerParser = ConfigKVParser.EntryParser.Value.create("reasoner", "Configure reasoner debugger.", new ReasonerParser());
            private static final Set<ConfigKVParser.EntryParser<?>> entryParsers = set(reasonerParser);

            @Override
            public Config.Log.Debugger parse(Yaml yaml, String scope) {
                if (yaml.isMap()) {
                    validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                    return new Config.Log.Debugger(reasonerParser.parse(yaml.asMap(), scope));
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
            }

            @Override
            public List<CommandLine.Option.CliHelp.Help> help(String scope) {
                return list(reasonerParser.help(scope));
            }

            static class ReasonerParser extends Nested<Config.Log.Debugger.Reasoner> {

                static final String type = "reasoner";
                private static final ConfigKVParser.EntryParser<String> typeParser = ConfigKVParser.EntryParser.EnumValue.create("type", "Type of this debugger.", STRING, list(type));
                private static final ConfigKVParser.EntryParser<String> outputParser = ConfigKVParser.EntryParser.Value.create("output", "Name of output reasoner debugger should write to (must be directory).", STRING);
                private static final ConfigKVParser.EntryParser<Boolean> enableParser = ConfigKVParser.EntryParser.Value.create("enable", "Enable to allow reasoner debugging to be enabled at runtime.", BOOLEAN);
                private static final Set<ConfigKVParser.EntryParser<?>> entryParsers = set(typeParser, outputParser, enableParser);

                @Override
                public Config.Log.Debugger.Reasoner parse(Yaml yaml, String scope) {
                    if (yaml.isMap()) {
                        validatedRecognisedParsers(entryParsers, yaml.asMap().keys(), scope);
                        return new Config.Log.Debugger.Reasoner(typeParser.parse(yaml.asMap(), scope), outputParser.parse(yaml.asMap(), scope),
                                enableParser.parse(yaml.asMap(), scope));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, scope);
                }

                @Override
                public List<CommandLine.Option.CliHelp.Help> help(String scope) {
                    return list(typeParser.help(scope), outputParser.help(scope), enableParser.help(scope));
                }
            }
        }
    }

    public static class VaticleFactoryParser extends ConfigKVParser.ValueParser.Nested<Config.VaticleFactory> {

        public static final String name = "vaticle-factory";
        public static final String description = "Configure Vaticle Factory connection.";

        private static final ConfigKVParser.EntryParser<Boolean> enableParser = ConfigKVParser.EntryParser.Value.create("enable", "Enable Vaticle Factory tracing.", BOOLEAN);
        private static final ConfigKVParser.EntryParser<String> uriParser = ConfigKVParser.EntryParser.Value.create("uri", "URI of Vaticle Factory server.", STRING);
        private static final ConfigKVParser.EntryParser<String> usernameParser = ConfigKVParser.EntryParser.Value.create("username", "Username for Vaticle Factory server.", STRING);
        private static final ConfigKVParser.EntryParser<String> tokenParser = ConfigKVParser.EntryParser.Value.create("token", "Authentication token for Vaticle Factory server.", STRING);
        private static final Set<ConfigKVParser.EntryParser<?>> entryParsers = set(enableParser, uriParser, usernameParser, tokenParser);

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
        public List<CommandLine.Option.CliHelp.Help> help(String scope) {
            return list(enableParser.help(scope), uriParser.help(scope), usernameParser.help(scope), tokenParser.help(scope));
        }
    }

    private static void validatedRecognisedParsers(Set<ConfigKVParser.EntryParser<?>> parsers, Set<String> keys, String scope) {
        Set<String> unrecognisedKeys = new HashSet<>(keys);
        parsers.forEach(parser -> unrecognisedKeys.remove(parser.key()));
        if (!unrecognisedKeys.isEmpty()) {
            Set<String> scopedKeys = iterate(unrecognisedKeys).map(key -> scopeKey(scope, key)).toSet();
            throw TypeDBException.of(UNRECOGNISED_CONFIGURATION_OPTIONS, scopedKeys);
        }
    }

}
