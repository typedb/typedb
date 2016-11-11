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

import ai.grakn.engine.loader.RESTLoader;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.util.REST;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.UUID;

import static spark.Spark.get;
import static spark.Spark.post;


@Path("/transaction")
@Api(value = "/transaction", description = "Endpoints to load new batched Graql queries and query the status of a specific transaction.")
@Produces("text/plain")
public class TransactionController {

    RESTLoader loader;
    String defaultGraphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
    private final Logger LOG = LoggerFactory.getLogger(TransactionController.class);


    public TransactionController() {

        loader = RESTLoader.getInstance();

        get(REST.WebPath.LOADER_STATE_URI, this::loaderState);
        post(REST.WebPath.NEW_TRANSACTION_URI, this::newTransactionREST);
        get(REST.WebPath.TRANSACTION_STATUS_URI + REST.Request.UUID_PARAMETER, this::checkTransactionStatusREST);

    }

    @POST
    @Path("/new")
    @ApiOperation(
            value = "Load a new transaction made of Graql insert queries into the graph.",
            notes = "The body of the request should be a JSON array of the insert Graql strings.")
    @ApiImplicitParam(name = "graphName", value = "Name of graph to use", dataType = "string", paramType = "query")
    private String newTransactionREST(Request req, Response res) {
        String currentGraphName = req.queryParams(REST.Request.GRAPH_NAME_PARAM);
        if (currentGraphName == null) currentGraphName = defaultGraphName;
        UUID uuid = loader.addJob(currentGraphName, Json.read(req.body()));
        if (uuid != null) {
            res.status(201);
            return uuid.toString();
        } else {
            throw new GraknEngineServerException(500,"Error while trying to load a new transaction.");
        }
    }


    @GET
    @Path("/status/:uuid")
    @ApiOperation(
            value = "Returns the status of the transaction associated to the given UUID.")
    @ApiImplicitParam(name = "uuid", value = "UUID of the transaction", required = true, dataType = "string", paramType = "path")
    private String checkTransactionStatusREST(Request req, Response res) {
        try {
            return loader.getStatus(UUID.fromString(req.params(REST.Request.UUID_PARAMETER)));
        } catch (Exception e) {
            throw new GraknEngineServerException(500,e);
        }
    }

    @GET
    @Path("/loaderState")
    @ApiOperation(
            value = "Returns the state of the RESTLoader.")
    private String loaderState(Request req, Response res) {
        try {
            return loader.getLoaderState();
        } catch (Exception e) {
            throw new GraknEngineServerException(500,e);
        }
    }
}