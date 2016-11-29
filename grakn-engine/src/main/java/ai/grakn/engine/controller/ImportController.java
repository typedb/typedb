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

import ai.grakn.engine.loader.Loader;
import ai.grakn.engine.loader.LoaderImpl;
import ai.grakn.engine.loader.client.LoaderClient;
import ai.grakn.engine.postprocessing.PostProcessing;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.internal.parser.QueryParser;
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static ai.grakn.engine.controller.Utilities.getAsList;
import static ai.grakn.engine.controller.Utilities.getAsString;
import static ai.grakn.engine.controller.Utilities.getKeyspace;
import static spark.Spark.before;
import static spark.Spark.halt;
import static spark.Spark.post;
import static ai.grakn.util.REST.Request.PATH_FIELD;
import static ai.grakn.util.REST.Request.HOSTS_FIELD;
import static java.util.concurrent.TimeUnit.SECONDS;

@Api(value = "/import", description = "Endpoints to import Graql data from a file.")
@Path("/import")
@Produces("text/plain")
public class ImportController {

    private final Logger LOG = LoggerFactory.getLogger(ImportController.class);
    private final AtomicBoolean loadingInProgress = new AtomicBoolean(false);

    private static final String INSERT_KEYWORD = "insert";
    private static final String MATCH_KEYWORD = "match";

    public ImportController() {
        before(REST.WebPath.IMPORT_DATA_URI, (req, res) -> {
            if (loadingInProgress.get())
                halt(423, "Another loading process is still running.\n");
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
            @ApiImplicitParam(name = "hosts", value = "Collection of hosts' addresses.", required = true, dataType = "string", paramType = "body"),
            @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query")
    })
    private String importDataREST(Request req, Response res) {
        try {
            final String keyspace = getKeyspace(req);
            final String pathToFile = getAsString(PATH_FIELD, req.body());
            final Collection<String> hosts = getAsList(HOSTS_FIELD, req.body());

            final File file = new File(pathToFile);
            if (!file.exists())
                throw new FileNotFoundException(ErrorMessage.NO_GRAQL_FILE.getMessage(pathToFile));

            Loader loader = getLoader(keyspace, hosts);

            // Spawn threads to load and check status of loader
            ScheduledFuture scheduledFuture = scheduledPrinting(loader);
            Executors.newSingleThreadExecutor().submit(() -> importDataFromFile(file, loader, scheduledFuture));

        } catch (FileNotFoundException j) {
            throw new GraknEngineServerException(400, j);
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }

        return "Loading successfully STARTED.\n";
    }

    /**
     * Return the appropriate loader- Blocking if no hosts are provided, distributed otherwise
     * @param keyspace name of the graph to use
     * @param hosts collection of hosts' addressed
     * @return Loader configured to the provided keyspace
     */
    private Loader getLoader(String keyspace, Collection<String> hosts){
        return hosts == null ? new LoaderImpl(keyspace) :new LoaderClient(keyspace, hosts);
    }

    /**
     * Spawn a thread that will check the status of the loader every 10 seconds.
     * @param loader loader the task will print the status of
     * @return a ScheduledFuture representing printing task
     */
    private ScheduledFuture scheduledPrinting(Loader loader){
        return Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(loader::printLoaderState, 10, 10, SECONDS);
    }

    // This method works under the following assumption:
    // - all entities insert statements are before the relation ones
    private void importDataFromFile(File file, Loader loaderParam, Future statusPrinter) {
        LOG.info("Data loading started.");
        loadingInProgress.set(true);
        try {
            Iterator<Object> batchIterator = QueryParser.create(Graql.withoutGraph()).parseBatchLoad(new FileInputStream(file)).iterator();
            if (batchIterator.hasNext()) {
                Object var = batchIterator.next();
                // -- ENTITIES --
                while (var.equals(INSERT_KEYWORD)) {
                    var = consumeInsertEntity(batchIterator, loaderParam);
                }
                loaderParam.waitToFinish();
                // ---- RELATIONS --- //
                while (var.equals(MATCH_KEYWORD)) {
                    var = consumeInsertRelation(batchIterator, loaderParam);
                }
            }

            PostProcessing.getInstance().run();
        } catch (Exception e) {
            LOG.error("Exception while batch loading data.", e);
        } finally {
            statusPrinter.cancel(true);
            loadingInProgress.set(false);
        }
    }

    private Object consumeInsertEntity(Iterator<Object> batchIterator, Loader loader) {
        Object var = null;
        List<Var> insertQuery = new ArrayList<>();
        while (batchIterator.hasNext()) {
            var = batchIterator.next();
            if (var instanceof Var) {
                insertQuery.add(((Var) var));
            } else
                break;
        }
        loader.add(Graql.insert(insertQuery));

        return var;
    }

    private Object consumeInsertRelation(Iterator<Object> batchIterator, Loader loader) {
        Object var = null;
        List<Var> insertQueryMatch = new ArrayList<>();
        while (batchIterator.hasNext()) {
            var = batchIterator.next();
            if (var instanceof Var) {
                insertQueryMatch.add(((Var) var));
            } else
                break;
        }
        List<Var> insertQuery;
        if (!var.equals(INSERT_KEYWORD))
            throw new GraknEngineServerException(500, "Match statement not followed by any Insert.");

        insertQuery = new ArrayList<>();
        while (batchIterator.hasNext()) {
            var = batchIterator.next();
            if (var instanceof Var) {
                insertQuery.add(((Var) var));
            } else
                break;
        }


        loader.add(Graql.match(insertQueryMatch).insert(insertQuery));

        return var;
    }
}
