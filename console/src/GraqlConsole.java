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

package grakn.core.console;

import grakn.core.server.Keyspace;
import grakn.core.client.Grakn;
import grakn.core.util.ErrorMessage;
import grakn.core.util.GraknVersion;
import grakn.core.util.SimpleURI;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.grpc.Status;
import jline.console.ConsoleReader;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;

import static grakn.core.console.GraqlShell.loadQuery;

/**
 *
 *  Graql console class that executes actions associated to options, if any option is set
 *  otherwise instantiates a GraqlShell
 *
 */

public class GraqlConsole {

    public static boolean start(
            GraqlShellOptions options, String historyFile,
            PrintStream sout, PrintStream serr
    ) throws InterruptedException, IOException {

        List<String> queries = null;



        //  ------- Check option  ------------------------
        String query = options.getQuery();

        // This is a best-effort guess as to whether the user has made a mistake, without parsing the query
        if (query != null) {
            queries = ImmutableList.of(query);

            if (!query.contains("$") && query.trim().startsWith("match")) {
                serr.println(ErrorMessage.NO_VARIABLE_IN_QUERY.getMessage());
            }
        }


        //  ------- Check option  ------------------------

        // Print usage message if requested or if invalid arguments provided
        if (options.displayHelp()) {
            options.printUsage(sout);
            return true;
        }

        //  ------- Check option  ------------------------

        if (options.displayVersion()) {
            sout.println(GraknVersion.VERSION);
            return true;
        }


        //   --------   If no option set we start GraqlShell   ----------

        OutputFormat outputFormat = options.getOutputFormat();

        boolean infer = options.shouldInfer();
        ConsoleReader console = new ConsoleReader(System.in, sout);
        SimpleURI defaultGrpcUri = Grakn.DEFAULT_URI;
        SimpleURI location = options.getUri();

        SimpleURI uri = location != null ? location : defaultGrpcUri;
        Keyspace keyspace = options.getKeyspace();
        Grakn client = new Grakn(uri);

        try (GraqlShell shell = new GraqlShell(historyFile, client, keyspace, console, serr, outputFormat, infer)) {
            List<Path> filePaths = options.getFiles();
            if (filePaths != null) {
                queries = loadQueries(filePaths);
            }

            // Start shell
            shell.start(queries);
            return !shell.errorOccurred();
        } catch (RuntimeException e) {
            if (e.getMessage().startsWith(Status.Code.UNAVAILABLE.name())) {
                serr.println(ErrorMessage.COULD_NOT_CONNECT.getMessage());
            } else {
                serr.println(e.getMessage());
            }
            return false;
        }
    }

    private static List<String> loadQueries(Iterable<Path> filePaths) throws IOException {
        List<String> queries = Lists.newArrayList();

        for (Path filePath : filePaths) {
            queries.add(loadQuery(filePath));
        }

        return queries;
    }
}
