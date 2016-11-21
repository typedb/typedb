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

import ai.grakn.engine.loader.BlockingLoader;
import ai.grakn.engine.loader.DistributedLoader;
import ai.grakn.engine.loader.Loader;
import ai.grakn.engine.postprocessing.PostProcessing;
import ai.grakn.engine.util.ConfigProperties;
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
import org.json.JSONException;
import org.json.JSONObject;
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
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static spark.Spark.*;


@Api(value = "/import", description = "Endpoints to import data and ontologies from Graqlfiles to a graph.")
@Path("/import")
@Produces("text/plain")

public class ImportController {

    private final Logger LOG = LoggerFactory.getLogger(ImportController.class);
    private ScheduledExecutorService checkLoadingExecutor = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture printingState;

    private AtomicLong processedEntities = new AtomicLong();
    private AtomicLong processedRelations = new AtomicLong();
    private AtomicBoolean loadingInProgress = new AtomicBoolean(false);

    private static final String INSERT_KEYWORD = "insert";
    private static final String MATCH_KEYWORD = "match";


    private String defaultGraphName;

    public ImportController() {

        before(REST.WebPath.IMPORT_DATA_URI, (req, res) -> {
            if (loadingInProgress.get())
                halt(423, "Another loading process is still running.\n");
        });
        before(REST.WebPath.IMPORT_DISTRIBUTED_URI, (req, res) -> {
            if (loadingInProgress.get())
                halt(423, "Another loading process is still running.\n");
        });

        post(REST.WebPath.IMPORT_DATA_URI, this::importDataREST);
        post(REST.WebPath.IMPORT_DISTRIBUTED_URI, this::importDataRESTDistributed);

        defaultGraphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_KEYSPACE_PROPERTY);
    }

    @POST
    @Path("/distribute/data")
    @ApiOperation(
            value = "Import data from a Graql file. It performs batch loading and distributed the batches to remote hosts.",
            notes = "This is a separate import from ontology, since a batch loading is performed to optimise the loading speed. ")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "path", value = "File path on the server.", required = true, dataType = "string", paramType = "body"),
            @ApiImplicitParam(name = "hosts", value = "Collection of hosts' addresses.", required = true, dataType = "string", paramType = "body")
    })

    private String importDataRESTDistributed(Request req, Response res) {
        loadingInProgress.set(true);
        try {
            JSONObject bodyObject = new JSONObject(req.body());
            final String pathToFile = bodyObject.get(REST.Request.PATH_FIELD).toString();
            final String graphName = (bodyObject.has(REST.Request.KEYSPACE_PARAM)) ? bodyObject.get(REST.Request.KEYSPACE_PARAM).toString() : defaultGraphName;
            final Collection<String> hosts = new HashSet<>();
            bodyObject.getJSONArray("hosts").forEach(x -> hosts.add(((String) x)));

            if (!(new File(pathToFile)).exists())
                throw new FileNotFoundException(ErrorMessage.NO_GRAQL_FILE.getMessage(pathToFile));

            Executors.newSingleThreadExecutor().submit(() -> importDataFromFile(pathToFile, new DistributedLoader(graphName, hosts)));

        } catch (JSONException | FileNotFoundException j) {
            loadingInProgress.set(false);
            throw new GraknEngineServerException(400, j);
        } catch (Exception e) {
            loadingInProgress.set(false);
            throw new GraknEngineServerException(500, e);
        }

        return "Distributed loading successfully STARTED. \n";
    }


    @POST
    @Path("/batch/data")
    @ApiOperation(
            value = "Import data from a Graql file. It performs batch loading.",
            notes = "This is a separate import from ontology, since a batch loading is performed to optimise the loading speed. ")
    @ApiImplicitParam(name = "path", value = "File path on the server.", required = true, dataType = "string", paramType = "body")

    private String importDataREST(Request req, Response res) {
        loadingInProgress.set(true);
        try {
            JSONObject bodyObject = new JSONObject(req.body());
            final String pathToFile = bodyObject.get(REST.Request.PATH_FIELD).toString();
            final String graphName = (bodyObject.has(REST.Request.KEYSPACE_PARAM)) ? bodyObject.get(REST.Request.KEYSPACE_PARAM).toString() : defaultGraphName;

            if (!(new File(pathToFile)).exists())
                throw new FileNotFoundException(ErrorMessage.NO_GRAQL_FILE.getMessage(pathToFile));

            initialiseLoading();

            Executors.newSingleThreadExecutor().submit(() -> importDataFromFile(pathToFile, new BlockingLoader(graphName)));

        } catch (JSONException | FileNotFoundException j) {
            loadingInProgress.set(false);
            throw new GraknEngineServerException(400, j);
        } catch (Exception e) {
            loadingInProgress.set(false);
            throw new GraknEngineServerException(500, e);
        }

        return "Loading successfully STARTED. \n";
    }

    private void initialiseLoading() {
        printingState = checkLoadingExecutor.scheduleAtFixedRate(this::checkLoadingStatus, 10, 10, TimeUnit.SECONDS);
        processedEntities.set(0);
        processedRelations.set(0);
    }

    private void checkLoadingStatus() {
        LOG.info("===== Import from file in progress ====");
        LOG.info("Processed Entities: " + processedEntities);
        LOG.info("Processed Relations: " + processedRelations);
        LOG.info("=======================================");
    }


    // This method works under the following assumption:
    // - all entities insert statements are before the relation ones

    private void importDataFromFile(String dataFile, Loader loaderParam) {
        LOG.info("Data loading started.");
        try {
            Iterator<Object> batchIterator = QueryParser.create(Graql.withoutGraph()).parseBatchLoad(new FileInputStream(dataFile)).iterator();
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
            loaderParam.waitToFinish();
            LOG.info("Data loading complete:");
            checkLoadingStatus();
            printingState.cancel(true);
            processedEntities.set(0);
            processedRelations.set(0);
            loadingInProgress.set(false);
            PostProcessing.getInstance().run();
        } catch (Exception e) {
            LOG.error("Exception while batch loading data.", e);
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
                processedEntities.incrementAndGet();
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
        processedRelations.incrementAndGet();

        return var;
    }
}
