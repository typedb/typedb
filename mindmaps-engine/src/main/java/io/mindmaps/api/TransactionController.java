/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.api;

import io.mindmaps.constants.RESTUtil;
import io.mindmaps.loader.RESTLoader;
import io.mindmaps.util.ConfigProperties;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
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

    public TransactionController() {

        loader = RESTLoader.getInstance();

        post(RESTUtil.WebPath.NEW_TRANSACTION_URI, this::newTransactionREST);
        get(RESTUtil.WebPath.TRANSACTION_STATUS_URI, this::checkTransactionStatusREST);

    }
    @POST
    @Path("/new")
    @ApiOperation(
            value = "Load a new transaction made of Graql insert queries into the graph.",
            notes = "The body of the request must only contain the insert Graql strings.",
            response = String.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "graphName", value = "Name of graph tu use", dataType = "string", paramType = "query")
    })
    private String newTransactionREST(Request req, Response res){
        String currentGraphName = req.queryParams(RESTUtil.Request.GRAPH_NAME_PARAM);
        if (currentGraphName == null) currentGraphName = defaultGraphName;
        UUID uuid = loader.addJob(currentGraphName, req.body());
        if (uuid != null) {
            res.status(201);
            return uuid.toString();
        } else {
            res.status(405);
            return "Error";
        }
    }

    @GET
    @Path("/status/:uuid")
    @ApiOperation(
            value = "Returns the status of the transaction associated to the given UUID.",
            response = String.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "uuid", value = "UUID of the transaction", required = true, dataType = "string", paramType = "path")
    })
    private String checkTransactionStatusREST(Request req, Response res){
        try {
            return loader.getStatus(UUID.fromString(req.params(RESTUtil.Request.UUID_PARAMETER))).toString();
        } catch (Exception e) {
            e.printStackTrace();
            res.status(400);
            return e.getMessage();
        }
    }
}