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

import com.vaticle.typedb.common.yaml.YAML;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.common.Util;
import com.vaticle.typedb.core.server.parameters.util.YAMLParser;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIGS_UNRECOGNISED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_SECTION_MUST_BE_MAP;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_YAML_MUST_BE_MAP;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.server.common.Constants.DIAGNOSTICS_REPORTING_URI;
import static com.vaticle.typedb.core.server.common.Constants.METRICS_REPORTING_URI;
import static com.vaticle.typedb.core.server.common.Constants.TYPEDB_LOG_FILE_EXT;
import static com.vaticle.typedb.core.server.common.Constants.TYPEDB_LOG_FILE_NAME;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.KeyValue.Dynamic;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.KeyValue.Predefined;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.Value.Primitive.BOOLEAN;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.Value.Primitive.BYTES_SIZE;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.Value.Primitive.INET_SOCKET_ADDRESS;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.Value.Primitive.INTEGER;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.Value.Primitive.LIST_STRING;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.Value.Primitive.PATH;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.Value.Primitive.STRING;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.Value.Primitive.TIME_PERIOD;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.Value.Primitive.TIME_PERIOD_NAME;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.dynamic;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.predefined;
import static com.vaticle.typedb.core.server.parameters.util.YAMLParser.restricted;

public class CoreConfigParser extends YAMLParser.Value.Compound<CoreConfig> {

    private static final Predefined<CoreConfig.Server> server = predefined(Server.name, Server.description, new Server());
    private static final Predefined<CoreConfig.Storage> storage = predefined(Storage.name, Storage.description, new Storage());
    private static final Predefined<CoreConfig.Log> log = predefined(Log.name, Log.description, new Log());
    protected static final Predefined<CoreConfig.Diagnostics> diagnostics =
            predefined(Diagnostics.name, Diagnostics.description, new Diagnostics());
    protected static final Predefined<CoreConfig.Metrics> metrics =
            predefined(Metrics.name, Metrics.description, new Metrics());
    protected static final Predefined<CoreConfig.VaticleFactory> vaticleFactory =
            predefined(VaticleFactory.name, VaticleFactory.description, new VaticleFactory());
    private static final Set<Predefined<?>> parsers = set(server, storage, log, diagnostics, metrics, vaticleFactory);

    @Override
    public CoreConfig parse(YAML yaml, String path) {
        if (yaml.isMap()) {
            validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
            return new CoreConfig(server.parse(yaml.asMap(), path), storage.parse(yaml.asMap(), path),
                    log.parse(yaml.asMap(), path), diagnostics.parse(yaml.asMap(), path), metrics.parse(yaml.asMap(), path),
                    vaticleFactory.parse(yaml.asMap(), path)
            );
        } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
    }

    @Override
    public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
        return list(server.help(path), storage.help(path), log.help(path), vaticleFactory.help(path));
    }

    protected static Path configPathAbsolute(Path path) {
        if (path.isAbsolute()) return path;
        else return Util.getTypedbDir().resolve(path);
    }

    protected static class Common {

        public static class Output extends Compound<CoreConfig.Common.Output> {

            public static final String name = "output";
            public static final String description = "Log output definitions.";

            private static final Dynamic<CoreConfig.Common.Output.Type> type = dynamic(Type.description, new Type());

            @Override
            public CoreConfig.Common.Output parse(YAML yaml, String path) {
                if (yaml.isMap()) return new CoreConfig.Common.Output(type.parseFrom(yaml.asMap(), path));
                else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
            }

            @Override
            public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                return list(type.help(path));
            }

            protected static class Type extends Compound<CoreConfig.Common.Output.Type> {

                private static final String description = "A named log output definition.";
                private static final Predefined<String> type = predefined(
                        "type", "Type of output to define.", restricted(STRING, list(Stdout.type, File.type))
                );
                protected static final Compound<CoreConfig.Common.Output.Type.Stdout> stdout = new Stdout();
                protected static final Compound<CoreConfig.Common.Output.Type.File> file = new File(TYPEDB_LOG_FILE_NAME, TYPEDB_LOG_FILE_EXT);

                @Override
                public CoreConfig.Common.Output.Type parse(YAML yaml, String path) {
                    if (yaml.isMap()) {
                        String value = type.parse(yaml.asMap(), path);
                        switch (value) {
                            case Stdout.type:
                                return stdout.parse(yaml.asMap(), path);
                            case File.type:
                                return file.parse(yaml.asMap(), path).asFile();
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

                protected static class Stdout extends Compound<CoreConfig.Common.Output.Type.Stdout> {

                    public static final String type = "stdout";
                    public static final String description = "Options to configure a log output to stdout.";

                    private static final Predefined<String> typeParser = predefined(
                            "type", "An output that writes to stdout.", restricted(STRING, list(type))
                    );
                    private static final Predefined<Boolean> enable =
                            predefined("enable", "Enable logging to stdout.", BOOLEAN);
                    private static final Set<Predefined<?>> parsers = set(typeParser, enable);

                    @Override
                    public CoreConfig.Common.Output.Type.Stdout parse(YAML yaml, String path) {
                        if (yaml.isMap()) {
                            validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                            String type = typeParser.parse(yaml.asMap(), path);
                            assert Stdout.type.equals(type);
                            boolean enabled = enable.parse(yaml.asMap(), path);
                            return new CoreConfig.Common.Output.Type.Stdout(enabled);
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                    }

                    @Override
                    public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                        return list(typeParser.help(path), enable.help(path));
                    }
                }

                protected static class File extends Compound<CoreConfig.Common.Output.Type.File> {

                    public static final String type = "file";
                    public static final String description = "Options to configure a log output to files in a directory.";

                    private static final Predefined<String> typeParser =
                            predefined("type", "An output that writes to a directory.", restricted(STRING, list(type)));
                    private static final Predefined<Boolean> enable =
                            predefined("enable", "Enable logging to the file.", BOOLEAN);
                    private static final Predefined<Path> baseDirectory =
                            predefined("base-dir", "Directory to write to. Relative paths are relative to distribution path.", PATH);
                    private static final Predefined<Long> fileSizeLimit =
                            predefined("file-size-limit", "Active log file size limit before creating new file (eg. 50mb).", BYTES_SIZE);
                    private static final Predefined<TimePeriodName> archiveGrouping =
                            predefined("archive-grouping", "Archive grouping and naming by time period (eg. month implies YYYY-MM)", TIME_PERIOD_NAME);
                    private static final Predefined<TimePeriod> archiveAgeLimit =
                            predefined("archive-age-limit", "Archive retention policy by age (eg. keep for 1 year)", TIME_PERIOD);
                    private static final Predefined<Long> archivesSizeLimit =
                            predefined(
                                    "archives-size-limit",
                                    "Size limit of all archived log files in directory (e.g., 1GB).",
                                    BYTES_SIZE
                            ); // TODO reasoner needs to respect this
                    private static final Set<Predefined<?>> parsers = set(
                            typeParser, enable, baseDirectory, fileSizeLimit, archiveGrouping, archiveAgeLimit, archivesSizeLimit
                    );

                    private final String filename;
                    private final String extension;

                    public File(String filename, String extension) {
                        super();
                        this.filename = filename;
                        this.extension = extension;
                    }

                    @Override
                    public CoreConfig.Common.Output.Type.File parse(YAML yaml, String path) {
                        if (yaml.isMap()) {
                            validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                            String type = typeParser.parse(yaml.asMap(), path);
                            assert File.type.equals(type);
                            return new CoreConfig.Common.Output.Type.File(
                                    enable.parse(yaml.asMap(), path),
                                    configPathAbsolute(baseDirectory.parse(yaml.asMap(), path)),
                                    filename,
                                    extension,
                                    fileSizeLimit.parse(yaml.asMap(), path),
                                    archiveGrouping.parse(yaml.asMap(), path),
                                    archiveAgeLimit.parse(yaml.asMap(), path),
                                    archivesSizeLimit.parse(yaml.asMap(), path)
                            );
                        } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                    }

                    @Override
                    public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                        return list(typeParser.help(path), enable.help(path), baseDirectory.help(path),
                                fileSizeLimit.help(path), archiveGrouping.help(path), archiveAgeLimit.help(path),
                                archivesSizeLimit.help(path));
                    }
                }
            }
        }
    }

    protected static class Server extends Compound<CoreConfig.Server> {

        public static final String name = "server";
        private static final String description = "Server and networking configuration.";

        protected static final Predefined<InetSocketAddress> address =
                predefined("address", "Address to listen for TypeDB Drivers on.", INET_SOCKET_ADDRESS);
        private static final Set<Predefined<?>> parsers = set(address);

        @Override
        public CoreConfig.Server parse(YAML yaml, String path) {
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

    protected static class Storage extends Compound<CoreConfig.Storage> {

        public static final String name = "storage";
        public static final String description = "Storage configuration.";

        protected static final Predefined<Path> data =
                predefined("data", "Directory in which user databases will be stored.", PATH);
        protected static final Predefined<CoreConfig.Storage.DatabaseCache> dbCache =
                predefined(DatabaseCache.name, DatabaseCache.description, new DatabaseCache());
        private static final Set<Predefined<?>> parsers = set(data, dbCache);

        @Override
        public CoreConfig.Storage parse(YAML yaml, String path) {
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
            public CoreConfig.Storage.DatabaseCache parse(YAML yaml, String path) {
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

    protected static class Log extends Compound<CoreConfig.Log> {

        public static final String name = "log";
        public static final String description = "Logging configuration.";

        private static final Predefined<CoreConfig.Common.Output> output =
                predefined(Common.Output.name, Common.Output.description, new Common.Output());
        protected static final Predefined<CoreConfig.Log.Logger> logger =
                predefined(Logger.name, Logger.description, new Logger());
        protected static final Predefined<CoreConfig.Log.Debugger> debugger =
                predefined(Debugger.name, Debugger.description, new Debugger());
        private static final Set<Predefined<?>> parsers = set(output, logger, debugger);

        @Override
        public CoreConfig.Log parse(YAML yaml, String path) {
            if (yaml.isMap()) {
                validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                CoreConfig.Common.Output output = Log.output.parse(yaml.asMap(), path);
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

        private static class Logger extends Compound<CoreConfig.Log.Logger> {

            private static final List<String> LEVELS = list("trace", "debug", "info", "warn", "error");

            private static final String name = "logger";
            private static final String description = "Loggers to activate.";

            private static final Predefined<CoreConfig.Log.Logger.Unfiltered> defaultLogger =
                    predefined("default", "The default logger.", new Unfiltered());
            private static final Dynamic<CoreConfig.Log.Logger.Filtered> filteredLogger =
                    dynamic("Custom filtered loggers.", new Filtered());

            @Override
            public CoreConfig.Log.Logger parse(YAML yaml, String path) {
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
                public CoreConfig.Log.Logger.Unfiltered parse(YAML yaml, String path) {
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
                public CoreConfig.Log.Logger.Filtered parse(YAML yaml, String path) {
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

            private static final Predefined<CoreConfig.Log.Debugger.ReasonerTracer> reasonerTracer =
                    predefined("reasoner-tracer", "Configure reasoner tracer.", new ReasonerTracer());

            private static final Predefined<CoreConfig.Log.Debugger.ReasonerPerfCounters> reasonerPerfCounters =
                    predefined("reasoner-perf-counters", "Configure transaction-bound reasoner performance counters.", new ReasonerPerfCounters());
            private static final Set<Predefined<?>> parsers = set(reasonerTracer, reasonerPerfCounters);

            @Override
            public CoreConfig.Log.Debugger parse(YAML yaml, String path) {
                if (yaml.isMap()) {
                    validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                    return new CoreConfig.Log.Debugger(reasonerTracer.parse(yaml.asMap(), path),
                            reasonerPerfCounters.parse(yaml.asMap(), path));
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
            }

            @Override
            public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                return list(reasonerTracer.help(path));
            }

            private static class ReasonerTracer extends Compound<CoreConfig.Log.Debugger.ReasonerTracer> {

                private static final String type = "reasoner-tracer";
                private static final Predefined<String> typeParser =
                        predefined("type", "Type of this debugger.", restricted(STRING, list(type)));
                private static final Predefined<String> output =
                        predefined("output", "Name of output reasoner debugger should write to (must be directory).", STRING);
                private static final Predefined<Boolean> enable =
                        predefined("enable", "Enable to allow reasoner debugging to be enabled at runtime.", BOOLEAN);
                private static final Set<Predefined<?>> parsers = set(typeParser, output, enable);

                @Override
                public CoreConfig.Log.Debugger.ReasonerTracer parse(YAML yaml, String path) {
                    if (yaml.isMap()) {
                        validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                        String type = typeParser.parse(yaml.asMap(), path);
                        assert ReasonerTracer.type.equals(type);
                        return new CoreConfig.Log.Debugger.ReasonerTracer(
                                output.parse(yaml.asMap(), path), enable.parse(yaml.asMap(), path));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                }

                @Override
                public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                    return list(typeParser.help(path), output.help(path), enable.help(path));
                }
            }

            private static class ReasonerPerfCounters extends Compound<CoreConfig.Log.Debugger.ReasonerPerfCounters> {

                private static final String type = "reasoner-perf-counters";
                private static final Predefined<String> typeParser =
                        predefined("type", "Type of this debugger.", restricted(STRING, list(type)));
                private static final Predefined<Boolean> enable =
                        predefined("enable", "Enable reasoner performance counters and logging in each transasction.", BOOLEAN);
                private static final Set<Predefined<?>> parsers = set(typeParser, output, enable);

                @Override
                public CoreConfig.Log.Debugger.ReasonerPerfCounters parse(YAML yaml, String path) {
                    if (yaml.isMap()) {
                        validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                        String type = typeParser.parse(yaml.asMap(), path);
                        assert ReasonerPerfCounters.type.equals(type);
                        return new CoreConfig.Log.Debugger.ReasonerPerfCounters(enable.parse(yaml.asMap(), path));
                    } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
                }

                @Override
                public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                    return list(typeParser.help(path), output.help(path), enable.help(path));
                }
            }
        }
    }

    protected static class Diagnostics extends Compound<CoreConfig.Diagnostics> {

        protected static final String name = "diagnostics";
        protected static final String description = "Configure server diagnostics.";

        protected static final Predefined<CoreConfig.Diagnostics.Reporting> reporting =
                predefined(Diagnostics.Reporting.name, Diagnostics.Reporting.description, new Diagnostics.Reporting());
        private static final Set<Predefined<?>> parsers = set(reporting);

        @Override
        public CoreConfig.Diagnostics parse(YAML yaml, String path) {
            if (yaml.isMap()) {
                validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                CoreConfig.Diagnostics.Reporting reporting = Diagnostics.reporting.parse(yaml.asMap(), path);
                return new CoreConfig.Diagnostics(reporting);
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
        }

        @Override
        public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
            return list(reporting.help(path));
        }

        protected static class Reporting extends Compound<CoreConfig.Diagnostics.Reporting> {

            protected static final String name = "reporting";
            protected static final String description = "Configure diagnostics reporting.";

            private static final Predefined<Boolean> enable =
                    predefined("enable", "Enable diagnostics reporting.", BOOLEAN);
            private static final Set<Predefined<?>> parsers = set(enable);

            @Override
            public CoreConfig.Diagnostics.Reporting parse(YAML yaml, String path) {
                if (yaml.isMap()) {
                    boolean enable = Reporting.enable.parse(yaml.asMap(), path);
                    validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                    if (enable) {
                        return new CoreConfig.Diagnostics.Reporting(true, DIAGNOSTICS_REPORTING_URI);
                    } else {
                        return new CoreConfig.Diagnostics.Reporting(false, null);
                    }
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
            }

            @Override
            public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                return list(enable.help(path));
            }
        }
    }

    protected static class Metrics extends Compound<CoreConfig.Metrics> {

        protected static final String name = "metrics";
        protected static final String description = "Configure server metrics.";

        protected static final Predefined<CoreConfig.Metrics.Reporting> reporting =
                predefined(Metrics.Reporting.name, Metrics.Reporting.description, new Metrics.Reporting());
        protected static final Predefined<CoreConfig.Metrics.ScrapeEndpoint> scrapeEndpoint =
                predefined(Metrics.ScrapeEndpoint.name, Metrics.ScrapeEndpoint.description, new Metrics.ScrapeEndpoint());
        private static final Set<Predefined<?>> parsers = set(reporting, scrapeEndpoint);

        @Override
        public CoreConfig.Metrics parse(YAML yaml, String path) {
            if (yaml.isMap()) {
                validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                CoreConfig.Metrics.Reporting reporting = Metrics.reporting.parse(yaml.asMap(), path);
                CoreConfig.Metrics.ScrapeEndpoint scrapeEndpoint = Metrics.scrapeEndpoint.parse(yaml.asMap(), path);
                return new CoreConfig.Metrics(reporting, scrapeEndpoint);
            } else throw TypeDBException.of(CONFIG_YAML_MUST_BE_MAP, path);
        }

        @Override
        public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
            return list(reporting.help(path));
        }

        protected static class Reporting extends Compound<CoreConfig.Metrics.Reporting> {

            protected static final String name = "reporting";
            protected static final String description = "Configure metrics reporting.";

            private static final Predefined<Boolean> enable =
                    predefined("enable", "Enable metrics reporting.", BOOLEAN);
            private static final Set<Predefined<?>> parsers = set(enable);

            @Override
            public CoreConfig.Metrics.Reporting parse(YAML yaml, String path) {
                if (yaml.isMap()) {
                    boolean enable = Reporting.enable.parse(yaml.asMap(), path);
                    validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                    if (enable) {
                        return new CoreConfig.Metrics.Reporting(true, METRICS_REPORTING_URI);
                    } else {
                        return new CoreConfig.Metrics.Reporting(false, null);
                    }
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
            }

            @Override
            public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                return list(enable.help(path));
            }
        }

        protected static class ScrapeEndpoint extends Compound<CoreConfig.Metrics.ScrapeEndpoint> {

            protected static final String name = "scrape-endpoint";
            protected static final String description = "Configure metrics scrape endpoint.";

            private static final Predefined<Boolean> enable =
                    predefined("enable", "Enable metrics scrape endpoint.", BOOLEAN);
            private static final Predefined<Integer> port =
                    predefined("port", "Port of the metrics scrape endpoint.", INTEGER);
            private static final Set<Predefined<?>> parsers = set(enable, port);

            @Override
            public CoreConfig.Metrics.ScrapeEndpoint parse(YAML yaml, String path) {
                if (yaml.isMap()) {
                    boolean enable = ScrapeEndpoint.enable.parse(yaml.asMap(), path);
                    validatePredefinedKeys(parsers, yaml.asMap().keys(), path);
                    if (enable) {
                        Integer port = ScrapeEndpoint.port.parse(yaml.asMap(), path);
                        return new CoreConfig.Metrics.ScrapeEndpoint(true, port);
                    } else {
                        return new CoreConfig.Metrics.ScrapeEndpoint(false, null);
                    }
                } else throw TypeDBException.of(CONFIG_SECTION_MUST_BE_MAP, path);
            }

            @Override
            public List<com.vaticle.typedb.core.server.parameters.util.Help> helpList(String path) {
                return list(enable.help(path));
            }
        }
    }

    protected static class VaticleFactory extends Compound<CoreConfig.VaticleFactory> {

        protected static final String name = "vaticle-factory";
        protected static final String description = "Configure Vaticle Factory connection.";

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
        public CoreConfig.VaticleFactory parse(YAML yaml, String path) {
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

    protected static void validatePredefinedKeys(Set<Predefined<?>> predefinedParsers, Set<String> keys, String path) {
        Set<String> unrecognisedKeys = new HashSet<>(keys);
        predefinedParsers.forEach(parser -> unrecognisedKeys.remove(parser.key()));
        if (!unrecognisedKeys.isEmpty()) {
            Set<String> childPaths = iterate(unrecognisedKeys).map(key -> YAMLParser.concatenate(path, key)).toSet();
            throw TypeDBException.of(CONFIGS_UNRECOGNISED, childPaths);
        }
    }
}
