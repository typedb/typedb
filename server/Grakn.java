/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.server;

import com.google.common.base.Stopwatch;
import grakn.core.common.config.SystemProperty;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.server.util.PIDManager;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * The main class of the 'grakn' command. This class is not a class responsible
 * for booting up the real command, but rather the command itself.
 *
 * Please keep the class name "Grakn" as it is what will be displayed to the user.
 */
public class Grakn {

    private static final Logger LOG = LoggerFactory.getLogger(Grakn.class);

    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler((Thread t, Throwable e) ->
                LOG.error(ErrorMessage.UNCAUGHT_EXCEPTION.getMessage(t.getName()), e));

        try {
            String graknPidFileProperty = Optional.ofNullable(SystemProperty.GRAKN_PID_FILE.value())
                    .orElseThrow(() -> new RuntimeException(ErrorMessage.GRAKN_PIDFILE_SYSTEM_PROPERTY_UNDEFINED.getMessage()));

            Path pidfile = Paths.get(graknPidFileProperty);
            PIDManager pidManager = new PIDManager(pidfile);
            pidManager.trackGraknPid();

            // Start Server with timer
            Stopwatch timer = Stopwatch.createStarted();
            Server server = ServerFactory.createServer(new Arguments(args));
            server.start();
            LOG.info("Grakn started in {}", timer.stop());
            try {
                server.awaitTermination();
            } catch (InterruptedException e) {
                // grakn server stop is called
                server.close();
                Thread.currentThread().interrupt();
            }
        } catch (RuntimeException | IOException e) {
            LOG.error(ErrorMessage.UNCAUGHT_EXCEPTION.getMessage(e.getMessage()), e);
            System.err.println(ErrorMessage.UNCAUGHT_EXCEPTION.getMessage(e.getMessage()));
        }
    }

    public static class Arguments {
        private final boolean benchmark;
        private final Tracing grablTracing;

        private Arguments(String[] args) {
            Options options = new Options();

            options.addOption(Option.builder("b")
                    .longOpt("benchmark")
                    .hasArg(false)
                    .desc("Enable benchmarking via Zipkin on the server")
                    .required(false)
                    .type(Boolean.class)
                    .build());

            options.addOption(Option.builder("t")
                    .longOpt("tracing")
                    .hasArg()
                    .desc("Enable grabl tracing")
                    .required(false)
                    .type(Boolean.class)
                    .build());

            options.addOption(Option.builder()
                    .longOpt("tracing-uri")
                    .hasArg()
                    .desc("Grabl tracing URI")
                    .required(false)
                    .type(String.class)
                    .build());

            options.addOption(Option.builder()
                    .longOpt("username")
                    .hasArg()
                    .desc("Grabl tracing username")
                    .required(false)
                    .type(String.class)
                    .build());

            options.addOption(Option.builder()
                    .longOpt("access-token")
                    .hasArg()
                    .desc("Grabl tracing access-token")
                    .required(false)
                    .type(String.class)
                    .build());

            CommandLineParser parser = new DefaultParser();
            CommandLine arguments;
            try {
                arguments = parser.parse(options, args);
            } catch (ParseException e) {
                (new HelpFormatter()).printHelp("Grakn options", options);
                throw new RuntimeException(e.getMessage());
            }

            benchmark = arguments.hasOption("benchmark");
            if (arguments.hasOption("tracing")) {
                if (benchmark) {
                    throw new RuntimeException("Cannot run with both benchmark and tracing at the same time");
                }
                grablTracing = new Tracing(
                        arguments.getOptionValue("tracing-uri"),
                        arguments.getOptionValue("username"),
                        arguments.getOptionValue("access-token")
                );
            } else {
                grablTracing = null;
            }
        }

        public boolean isBenchmark() {
            return benchmark;
        }

        public boolean isGrablTracing() {
            return grablTracing != null;
        }

        public Tracing getGrablTracing() {
            return grablTracing;
        }

        public static class Tracing {
            private final String uri;
            private final String username;
            private final String accessToken;

            private Tracing(String uri, String username, String accessToken) {
                this.uri = uri;
                this.username = username;
                this.accessToken = accessToken;
            }

            public String getUri() {
                return uri;
            }

            public String getUsername() {
                return username;
            }

            public String getAccessToken() {
                return accessToken;
            }
        }
    }
}

