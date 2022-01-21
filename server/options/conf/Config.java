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

package com.vaticle.typedb.core.server.options.conf;

import com.vaticle.typedb.core.common.exception.TypeDBException;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_OUTPUT_UNRECOGNISED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.CONFIG_REASONER_REQUIRES_DIR_OUTPUT;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class Config {

    protected final Server server;
    protected final Storage storage;
    protected final Log log;
    protected final VaticleFactory vaticleFactory;

    protected Config(Server server, Storage storage, Log log, @Nullable VaticleFactory vaticleFactory) {
        this.server = server;
        this.storage = storage;
        this.log = log;
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

    public VaticleFactory vaticleFactory() {
        return vaticleFactory;
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

        private final Output output;
        private final Logger logger;
        private final Debugger debugger;

        public Log(Output output, Logger logger, Debugger debugger) {
            this.output = output;
            this.logger = logger;
            this.debugger = debugger;
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

            private final Map<String, Type> outputs;

            public Output(Map<String, Type> outputs) {
                this.outputs = outputs;
            }

            public Map<String, Type> outputs() {
                return outputs;
            }

            public static abstract class Type {

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

                    Stdout(String type) {
                        assert type.equals(ConfigParser.LogParser.OutputParser.TypeParser.StdoutParser.type);
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

                    private final Path path;
                    private final long fileSizeCap;
                    private final long archivesSizeCap;

                    File(String type, Path path, long fileSizeCap, long archivesSizeCap) {
                        assert type.equals(ConfigParser.LogParser.OutputParser.TypeParser.FileParser.type);
                        this.path = path;
                        this.fileSizeCap = fileSizeCap;
                        this.archivesSizeCap = archivesSizeCap;
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

            private final Unfiltered defaultLogger;
            private final Map<String, Filtered> filteredLoggers;

            Logger(Unfiltered defaultLogger, Map<String, Filtered> filteredLoggers) {
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

            private final Reasoner reasoner;

            Debugger(Reasoner reasoner) {
                this.reasoner = reasoner;
            }

            public Reasoner reasoner() {
                return reasoner;
            }

            void validateAndSetOutputs(Map<String, Output.Type> outputs) {
                reasoner.validateAndSetOutputs(outputs);
            }

            public static class Reasoner {

                private final String outputName;
                private final boolean enable;
                private Output.Type.File output;

                Reasoner(String type, String outputName, boolean enable) {
                    assert type.equals(ConfigParser.LogParser.DebuggerParser.ReasonerParser.type);
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
