/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.server;

import grakn.core.server.bootup.EnginePidManager;
import grakn.core.server.bootup.GraknBootup;
import grakn.core.commons.config.SystemProperty;
import grakn.core.commons.exception.ErrorMessage;
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

    /**
     *
     * Invocation from class '{@link GraknBootup}'
     *
     * @param args
     */
    public static void main(String[] args) {

        Option enableBenchmark = Option.builder("b")
                .longOpt("benchmark")
                .hasArg(false)
                .desc("Enable benchmarking via Zipkin on the server")
                .required(false)
                .type(Boolean.class)
                .build();

        Options options = new Options();
        options.addOption(enableBenchmark);

        CommandLineParser parser = new DefaultParser();
        CommandLine arguments;
        try {
            arguments = parser.parse(options, args);
        } catch (ParseException e) {
            (new HelpFormatter()).printHelp("Grakn options", options);
            throw new RuntimeException(e.getMessage());
        }

        boolean benchmark = false;
        if (arguments.hasOption("benchmark")) {
            benchmark = true;
            System.out.println("Benchmarking enabled");
        }


        Thread.setDefaultUncaughtExceptionHandler((Thread t, Throwable e) ->
                LOG.error(ErrorMessage.UNCAUGHT_EXCEPTION.getMessage(t.getName()), e));

        try {
            String graknPidFileProperty = Optional.ofNullable(SystemProperty.GRAKN_PID_FILE.value())
                    .orElseThrow(() -> new RuntimeException(ErrorMessage.GRAKN_PIDFILE_SYSTEM_PROPERTY_UNDEFINED.getMessage()));

            Path pidfile = Paths.get(graknPidFileProperty);
            EnginePidManager enginePidManager = new EnginePidManager(pidfile);
            enginePidManager.trackGraknPid();

            // Start Engine
            Server server = ServerFactory.createServer(benchmark);
            server.start();
        } catch (RuntimeException | IOException e) {
            LOG.error(ErrorMessage.UNCAUGHT_EXCEPTION.getMessage(e.getMessage()), e);
            System.err.println(ErrorMessage.UNCAUGHT_EXCEPTION.getMessage(e.getMessage()));
        }
    }
}

