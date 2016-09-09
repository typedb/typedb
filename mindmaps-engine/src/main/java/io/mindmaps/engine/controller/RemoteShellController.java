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

package io.mindmaps.engine.controller;

import io.mindmaps.util.REST;
import io.mindmaps.graph.internal.AbstractMindmapsGraph;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.QueryParser;
import io.mindmaps.engine.session.RemoteSession;
import io.mindmaps.engine.util.ConfigProperties;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.stream.Collectors;

import static spark.Spark.*;


@Path("/shell")
@Api(value = "/shell", description = "Endpoints to execute match queries and obtain meta-ontology types instances.")
@Produces({"application/json", "text/plain"})

public class RemoteShellController {

    private final Logger LOG = LoggerFactory.getLogger(RemoteShellController.class);
    private final int WEBSOCKET_TIMEOUT = 600000;


    String defaultGraphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);

    public RemoteShellController() {

        webSocket(REST.WebPath.REMOTE_SHELL_URI, RemoteSession.class);
        webSocketIdleTimeoutMillis(WEBSOCKET_TIMEOUT);

        get(REST.WebPath.MATCH_QUERY_URI, this::matchQuery);
        get(REST.WebPath.META_TYPE_INSTANCES_URI, this::buildMetaTypeInstancesObject);
    }
    @GET
    @Path("/metaTypeInstances")
    @ApiOperation(
            value = "Produces a JSONObject containing meta-ontology types instances.",
            notes = "The built JSONObject will contain ontology nodes divided in roles, entities, relations and resources.",
            response = JSONObject.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "graphName", value = "Name of graph tu use", dataType = "string", paramType = "query")

    })
    private String buildMetaTypeInstancesObject(Request req, Response res){

        String currentGraphName = req.queryParams(REST.Request.GRAPH_NAME_PARAM);
        if (currentGraphName == null) currentGraphName = defaultGraphName;

        try {
            AbstractMindmapsGraph graph = (AbstractMindmapsGraph) GraphFactory.getInstance().getGraph(currentGraphName);

            JSONObject responseObj = new JSONObject();
            responseObj.put(REST.Response.ROLES_JSON_FIELD, new JSONArray(graph.getMetaRoleType().instances().stream().map(x -> x.getId()).toArray()));
            responseObj.put(REST.Response.ENTITIES_JSON_FIELD, new JSONArray(graph.getMetaEntityType().instances().stream().map(x -> x.getId()).toArray()));
            responseObj.put(REST.Response.RELATIONS_JSON_FIELD, new JSONArray(graph.getMetaRelationType().instances().stream().map(x -> x.getId()).toArray()));
            responseObj.put(REST.Response.RESOURCES_JSON_FIELD, new JSONArray(graph.getMetaResourceType().instances().stream().map(x -> x.getId()).toArray()));

            return responseObj.toString();
        }catch(Exception e){
            e.printStackTrace();
            res.status(500);
            return e.getMessage();
        }
    }

    @GET
    @Path("/match")
    @ApiOperation(
            value = "Executes match query on the server and produces a result string.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "graphName", value = "Name of graph to use", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "query", value = "Match query to execute", required = true,dataType = "string", paramType = "query")
    })
    private String matchQuery(Request req, Response res) {

        String currentGraphName = req.queryParams(REST.Request.GRAPH_NAME_PARAM);
        if(currentGraphName==null) currentGraphName = defaultGraphName;

        LOG.debug("Received match query: \"" + req.queryParams(REST.Request.QUERY_FIELD) + "\"");


        try {
            QueryParser parser = QueryParser.create(GraphFactory.getInstance().getGraph(currentGraphName));

            return parser.parseMatchQuery(req.queryParams(REST.Request.QUERY_FIELD))
                    .resultsString()
                    .map(x -> x.replaceAll("\u001B\\[\\d+[m]", ""))
                    .collect(Collectors.joining("\n"));
        } catch (Exception e) {
            e.printStackTrace();
            res.status(500);
            return e.getMessage();
        }
    }


}
