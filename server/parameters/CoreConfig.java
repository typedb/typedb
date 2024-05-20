/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.server.parameters;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.server.parameters.util.YAMLParser;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_LOG_OUTPUT_UNRECOGNISED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_REASONER_REQUIRES_DIR_OUTPUT;

public class CoreConfig {

    protected final Server server;
    protected final Storage storage;
    protected final Log log;
    protected final Diagnostics diagnostics;
    protected final VaticleFactory vaticleFactory;

    protected CoreConfig(Server server, Storage storage, Log log, @Nullable Diagnostics diagnostics, @Nullable VaticleFactory vaticleFactory) {
        this.server = server;
        this.storage = storage;
        this.log = log;
        this.diagnostics = diagnostics;
        this.vaticleFactory = vaticleFactory;
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

    public Diagnostics diagnostics() {
        return diagnostics;
    }

    public VaticleFactory vaticleFactory() {
        return vaticleFactory;
    }

    public static class Common {

        public static class Output {

            private final Map<String, Type> outputs;

            public Output(Map<String, Type> outputs) {
                this.outputs = outputs;
            }

            public Map<String, Type> outputs() {
                return outputs;
            }

            public static abstract class Type {

                private final boolean enable;

                protected Type(boolean enable) {
                    this.enable = enable;
                }

                public boolean enabled() {
                    return enable;
                }

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

                public static class Stdout extends Type {

                    Stdout(boolean enable) {
                        super(enable);
                    }

                    @Override
                    public boolean isStdout() {
                        return true;
                    }

                    @Override
                    public Stdout asStdout() {
                        return this;
                    }
                }

                public static class File extends Type {

                    private final Path baseDirectory;
                    private final long fileSizeLimit;
                    private final YAMLParser.Value.TimePeriodName archiveGrouping;
                    private final YAMLParser.Value.TimePeriod archiveAgeLimit;
                    private final long archivesSizeLimit;
                    private final String filename;
                    private final String extension;

                    File(boolean enable, Path baseDirectory, String filename, String extension,
                         long fileSizeLimit, YAMLParser.Value.TimePeriodName archiveGrouping,
                         YAMLParser.Value.TimePeriod archiveAgeLimit, long archivesSizeLimit) {
                        super(enable);
                        this.baseDirectory = baseDirectory;
                        this.filename = filename;
                        this.extension = extension;
                        this.fileSizeLimit = fileSizeLimit;
                        this.archiveGrouping = archiveGrouping;
                        this.archiveAgeLimit = archiveAgeLimit;
                        this.archivesSizeLimit = archivesSizeLimit;
                    }

                    public Path baseDirectory() {
                        return baseDirectory;
                    }

                    public String filename() {
                        return filename;
                    }

                    public String extension() {
                        return extension;
                    }

                    public long fileSizeLimit() {
                        return fileSizeLimit;
                    }

                    public YAMLParser.Value.TimePeriodName archiveGrouping() {
                        return archiveGrouping;
                    }

                    public YAMLParser.Value.TimePeriod archiveAgeLimit() {
                        return archiveAgeLimit;
                    }

                    public long archivesSizeLimit() {
                        return archivesSizeLimit;
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
    }

    public static class Server {

        private final InetSocketAddress address;

        protected Server(InetSocketAddress address) {
            this.address = address;
        }

        public InetSocketAddress address() {
            return address;
        }
    }

    public static class Storage {

        private final Path dataDir;
        private final DatabaseCache databaseCache;

        protected Storage(Path dataDir, DatabaseCache databaseCache) {
            this.dataDir = dataDir;
            this.databaseCache = databaseCache;
        }

        public Path dataDir() {
            return dataDir;
        }

        public DatabaseCache databaseCache() {
            return databaseCache;
        }

        public static class DatabaseCache {

            private final long dataSize;
            private final long indexSize;

            DatabaseCache(long dataSize, long indexSize) {
                this.dataSize = dataSize;
                this.indexSize = indexSize;
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

        private final Common.Output output;
        private final Logger logger;
        private final Debugger debugger;

        public Log(Common.Output output, Logger logger, Debugger debugger) {
            this.output = output;
            this.logger = logger;
            this.debugger = debugger;
        }

        public Common.Output output() {
            return output;
        }

        public Logger logger() {
            return logger;
        }

        public Debugger debugger() {
            return debugger;
        }

        public static class Logger {

            private final Unfiltered defaultLogger;
            private final Map<String, Filtered> filteredLoggers;

            Logger(Unfiltered defaultLogger, Map<String, Filtered> filteredLoggers) {
                this.defaultLogger = defaultLogger;
                this.filteredLoggers = filteredLoggers;
            }

            public void validateOutputs(Map<String, Common.Output.Type> outputs) {
                defaultLogger.validateOutputs(outputs);
                filteredLoggers.values().forEach(logger -> logger.validateOutputs(outputs));
            }

            public Unfiltered defaultLogger() {
                return defaultLogger;
            }

            public Map<String, Filtered> filteredLoggers() {
                return filteredLoggers;
            }

            public static class Unfiltered {

                private final String level;
                private final List<String> outputNames;

                Unfiltered(String level, List<String> outputNames) {
                    this.level = level;
                    this.outputNames = outputNames;
                }

                public String level() {
                    return level;
                }

                public List<String> outputs() {
                    return outputNames;
                }

                void validateOutputs(Map<String, Common.Output.Type> outputsAvailable) {
                    outputNames.forEach(name -> {
                        if (!outputsAvailable.containsKey(name)) {
                            throw TypeDBException.of(CONFIG_LOG_OUTPUT_UNRECOGNISED, name);
                        }
                    });
                }
            }

            public static class Filtered extends Unfiltered {

                private final String filter;

                Filtered(String level, List<String> outputNames, String filter) {
                    super(level, outputNames);
                    this.filter = filter;
                }

                public String filter() {
                    return filter;
                }
            }
        }

        public static class Debugger {

            private final ReasonerTracer reasonerTracer;
            private final ReasonerPerfCounters reasonerPerfCounters;

            Debugger(ReasonerTracer reasonerTracer, ReasonerPerfCounters reasonerPerfCounters) {
                this.reasonerTracer = reasonerTracer;
                this.reasonerPerfCounters = reasonerPerfCounters;
            }

            public ReasonerTracer reasonerTracer() {
                return reasonerTracer;
            }

            public void validateAndSetOutputs(Map<String, Common.Output.Type> outputs) {
                reasonerTracer.validateAndSetOutputs(outputs);
            }

            public ReasonerPerfCounters reasonerPerfCounters() {
                return reasonerPerfCounters;
            }

            public static class ReasonerTracer {

                private final String outputName;
                private final boolean enable;
                private Common.Output.Type.File output;

                ReasonerTracer(String outputName, boolean enable) {
                    this.outputName = outputName;
                    this.enable = enable;
                }

                public void validateAndSetOutputs(Map<String, Common.Output.Type> outputs) {
                    assert output == null;
                    if (!outputs.containsKey(outputName))
                        throw TypeDBException.of(CONFIG_LOG_OUTPUT_UNRECOGNISED, outputName);
                    else if (!outputs.get(outputName).isFile()) {
                        throw TypeDBException.of(CONFIG_REASONER_REQUIRES_DIR_OUTPUT);
                    }
                    output = outputs.get(outputName).asFile();
                }

                public boolean isEnabled() {
                    return enable;
                }

                public Common.Output.Type.File output() {
                    assert output != null;
                    return output;
                }
            }

            public static class ReasonerPerfCounters {

                private final boolean enable;

                ReasonerPerfCounters(boolean enable) {
                    this.enable = enable;
                }

                public boolean isEnabled() {
                    return enable;
                }
            }
        }
    }

    public static class Diagnostics {

        private final java.util.Optional<String> deploymentID;
        private final Reporting reporting;
        private final Monitoring monitoring;

        public Diagnostics(java.util.Optional<String> deploymentID, Reporting reporting, Monitoring monitoring) {
            this.deploymentID = deploymentID;
            this.reporting = reporting;
            this.monitoring = monitoring;
        }

        public Optional<String> deploymentID() {
            return deploymentID;
        }

        public Reporting reporting() {
            return reporting;
        }

        public Monitoring monitoring() {
            return monitoring;
        }

        public static class Reporting {

            private final boolean errors;
            private final boolean statistics;

            Reporting(boolean errors, boolean statistics) {
                this.errors = errors;
                this.statistics = statistics;
            }

            public boolean errors() {
                return errors;
            }

            public boolean statistics() {
                return statistics;
            }
        }

        public static class Monitoring {

            private final boolean enable;
            private final int port;

            public Monitoring(boolean enable, int port) {
                this.enable = enable;
                this.port = port;
            }

            public boolean enable() {
                return enable;
            }

            public int port() {
                return port;
            }
        }
    }

    /**
     * Until the scope expands, we take this to only mean configuration of vaticle-factory tracing
     */
    public static class VaticleFactory {

        private final boolean enable;
        private final String uri;
        private final String username;
        private final String token;

        VaticleFactory(boolean enable, @Nullable String uri, @Nullable String username, @Nullable String token) {
            this.enable = enable;
            this.uri = uri;
            this.username = username;
            this.token = token;
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
}
