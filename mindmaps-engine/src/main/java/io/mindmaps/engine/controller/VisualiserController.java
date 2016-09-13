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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.engine.visualiser.HALConcept;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.util.REST;
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

import static io.mindmaps.graql.Graql.withGraph;
import static spark.Spark.get;


@Path("/graph")
@Api(value = "/graph", description = "Endpoints used to query the graph by ID or Gralq match query and build HAL objects.")
@Produces({"application/json", "text/plain"})
public class VisualiserController {

    private final Logger LOG = LoggerFactory.getLogger(VisualiserController.class);

    private String defaultGraphName;

    public VisualiserController() {

        defaultGraphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);

        get(REST.WebPath.CONCEPT_BY_ID_URI + REST.Request.ID_PARAMETER, this::getConceptById);
        get(REST.WebPath.GRAPH_MATCH_QUERY_URI, this::matchQuery);
    }

    @GET
    @Path("/concept/:uuid")
    @ApiOperation(
            value = "Return the HAL representation of a given concept.")
    @ApiImplicitParam(name = "id", value = "ID of the concept", required = true, dataType = "string", paramType = "path")
    private String getConceptById(Request req, Response res) {

        String graphNameParam = req.queryParams(REST.Request.GRAPH_NAME_PARAM);
        String currentGraphName = (graphNameParam == null) ? defaultGraphName : graphNameParam;

        MindmapsGraph graph = GraphFactory.getInstance().getGraph(currentGraphName);

        Concept concept = graph.getConcept(req.params(REST.Request.ID_PARAMETER));
        if (concept != null) {
            LOG.debug("Building HAL resource for concept with id " + concept.getId().toString());
            return new HALConcept(concept).render();
        }
        else {
            res.status(404);
            return ErrorMessage.CONCEPT_ID_NOT_FOUND.getMessage(req.params(REST.Request.ID_PARAMETER));
        }
    }

    @GET
    @Path("/match")
    @ApiOperation(
            value = "Executes match query on the server and build HAL representation for each concept in the query result.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "graphName", value = "Name of graph to use", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "query", value = "Match query to execute", required = true, dataType = "string", paramType = "query")
    })
    private String matchQuery(Request req, Response res) {

        String currentGraphName = req.queryParams(REST.Request.GRAPH_NAME_PARAM);
        if (currentGraphName == null) currentGraphName = defaultGraphName;

        LOG.debug("Received match query: \"" + req.queryParams(REST.Request.QUERY_FIELD) + "\"");

        try {

            MindmapsGraph graph = GraphFactory.getInstance().getGraph(currentGraphName);
            final JSONArray halArray = new JSONArray();

            withGraph(graph).parseMatch(req.queryParams(REST.Request.QUERY_FIELD))
                    .getMatchQuery().stream()
                    .forEach(x -> x.values()
                            .forEach(concept -> {
                                LOG.debug("Building HAL resource for concept with id " + concept.getId().toString());
                                halArray.put(new JSONObject(new HALConcept(concept).render()));
                            }));
            LOG.debug("Done building resources.");
            return halArray.toString();
        } catch (Exception e) {
            e.printStackTrace();
            res.status(500);
            return e.getMessage();
        }
    }

}
