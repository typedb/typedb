/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.controller;

import ai.grakn.engine.backgroundtasks.TaskManager;
import ai.grakn.engine.loader.LoaderClient;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.REST;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.controller.Utilities.getAsString;
import static ai.grakn.engine.controller.Utilities.getKeyspace;
import static ai.grakn.util.REST.Request.PATH_FIELD;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static spark.Spark.before;
import static spark.Spark.halt;
import static spark.Spark.post;

/**
 * <p>
 *     Endpoints to import Graql data from a file.
 * </p>
 *
 * @author Marco Scoppetta, Felix Chapman, alexandraorth
 */
@Api(value = "/import", description = "Endpoints to import Graql data from a file.")
@Path("/import")
@Produces("text/plain")
public class ImportController {

    private final Logger LOG = LoggerFactory.getLogger(ImportController.class);
    private final AtomicBoolean loadingInProgress = new AtomicBoolean(false);
    private final TaskManager manager;

    public ImportController(TaskManager manager) {
        if (manager==null) {
            throw new GraknEngineServerException(500,"Task manager has not been instantiated.");
        }
        this.manager = manager;

        before(REST.WebPath.IMPORT_DATA_URI, (req, res) -> {
            if (loadingInProgress.get()) {
                halt(423, "Another loading process is still running.\n");
            }
        });

        post(REST.WebPath.IMPORT_DATA_URI, this::importDataREST);
    }

    @POST
    @Path("/batch/data")
    @ApiOperation(
            value = "Import data from a Graql file. It performs batch loading.",
            notes = "If the hosts field is populated, it will distribute the load amongst them. This should not " +
                    "be used to load ontologies because it splits up the file into smaller parts.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "path", value = "File path on the server.", required = true, dataType = "string", paramType = "body"),
            @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query")
    })
    private String importDataREST(Request req, Response res) {
        try {
            final String keyspace = getKeyspace(req);
            final String pathToFile = getAsString(PATH_FIELD, req.body());

            final File file = new File(pathToFile);
            if (!file.exists()) {
                throw new FileNotFoundException(ErrorMessage.NO_GRAQL_FILE.getMessage(pathToFile));
            }

//            LoaderClient loader = new LoaderClient(manager, keyspace);

            // Spawn threads to load and check status of loader
//            ScheduledFuture scheduledFuture = scheduledPrinting(loader);

            // Submit task to parse and load data in another thread
//            Thread importDataThread = new Thread(() -> importDataFromFile(file, loader, scheduledFuture));
//            importDataThread.start();

        } catch (FileNotFoundException j) {
            throw new GraknEngineServerException(400, j);
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }

        return "Loading successfully STARTED.\n";
    }

    /**
     * Spawn a thread that will check the status of the loader every 10 seconds.
     * @param loader loader the task will print the status of
     * @return a ScheduledFuture representing printing task
     */
//    private ScheduledFuture scheduledPrinting(LoaderClient loader){
//        return Executors.newSingleThreadScheduledExecutor()
//                .scheduleAtFixedRate(loader::printLoaderState, 10, 10, SECONDS);
//    }

//    private void importDataFromFile(File file, LoaderClient loader, Future statusPrinter) {
//        LOG.info("Data loading started.");
//        loadingInProgress.set(true);
//
//        try {
//            String fileAsString = Files.readAllLines(file.toPath()).stream().collect(joining("\n"));
//
//            Graql.withoutGraph()
//                    .parseList(fileAsString).stream()
//                    .map(p -> (InsertQuery) p)
//                    .forEach(loader::add);
//
//            loader.waitToFinish(60000);
//
//            LOG.info("Loading complete.");
//        } catch (IOException e) {
//            LOG.error("Exception while parsing data for batch load" + e);
//        } finally {
//            statusPrinter.cancel(true);
//            loadingInProgress.set(false);
//        }
//    }
}
