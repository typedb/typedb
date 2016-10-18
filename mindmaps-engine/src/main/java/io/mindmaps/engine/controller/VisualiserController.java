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

import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.engine.util.ConfigProperties;
import io.mindmaps.engine.visualiser.HALConcept;
import io.mindmaps.exception.MindmapsEngineServerException;
import io.mindmaps.factory.GraphFactory;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.internal.pattern.property.RelationProperty;
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
import java.util.*;
import java.util.stream.Collectors;

import static io.mindmaps.graql.Graql.withGraph;
import static spark.Spark.get;


@Path("/graph")
@Api(value = "/graph", description = "Endpoints used to query the graph by ID or Graql match query and build HAL objects.")
@Produces({"application/json", "text/plain"})
public class VisualiserController {

    private final Logger LOG = LoggerFactory.getLogger(VisualiserController.class);


    private String defaultGraphName;
    private int separationDegree;
    private final int MATCH_QUERY_FIXED_DEGREE = 0;
    //TODO: implement a pagination system instead of liming the result with hard-coded limit.
    private final int SAFETY_LIMIT = 500;

    public VisualiserController() {

        defaultGraphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        separationDegree = ConfigProperties.getInstance().getPropertyAsInt(ConfigProperties.HAL_DEGREE_PROPERTY);

        get(REST.WebPath.CONCEPT_BY_ID_URI + REST.Request.ID_PARAMETER, this::getConceptById);
        get(REST.WebPath.CONCEPT_BY_ID_ONTOLOGY_URI + REST.Request.ID_PARAMETER, this::getConceptByIdOntology);

        get(REST.WebPath.GRAPH_MATCH_QUERY_URI, this::matchQuery);

    }

    @GET
    @Path("/concept/:uuid")
    @ApiOperation(
            value = "Return the HAL representation of a given concept.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "ID of the concept", required = true, dataType = "string", paramType = "path"),
            @ApiImplicitParam(name = "graphName", value = "Name of graph to use", dataType = "string", paramType = "query")
    })
    private String getConceptById(Request req, Response res) {

        try {
            String graphNameParam = req.queryParams(REST.Request.GRAPH_NAME_PARAM);
            String currentGraphName = (graphNameParam == null) ? defaultGraphName : graphNameParam;

            MindmapsGraph graph = GraphFactory.getInstance().getGraph(currentGraphName);

            Concept concept = graph.getConcept(req.params(REST.Request.ID_PARAMETER));
            LOG.trace("Building HAL resource for concept with id {}", concept.getId());
            return new HALConcept(concept, separationDegree, false, new HashSet<>()).render();

        } catch (Exception e) {
            throw new MindmapsEngineServerException(500, e);
        }
    }

    @GET
    @Path("/concept/ontology/:uuid")
    @ApiOperation(
            value = "Return the HAL representation of a given concept.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "ID of the concept", required = true, dataType = "string", paramType = "path"),
            @ApiImplicitParam(name = "graphName", value = "Name of graph to use", dataType = "string", paramType = "query")
    })
    private String getConceptByIdOntology(Request req, Response res) {

        try {
            String graphNameParam = req.queryParams(REST.Request.GRAPH_NAME_PARAM);
            String currentGraphName = (graphNameParam == null) ? defaultGraphName : graphNameParam;

            MindmapsGraph graph = GraphFactory.getInstance().getGraph(currentGraphName);

            Concept concept = graph.getConcept(req.params(REST.Request.ID_PARAMETER));
            LOG.trace("Building HAL resource for concept with id {}", concept.getId());
            return new HALConcept(concept).render();

        } catch (Exception e) {
            throw new MindmapsEngineServerException(500, e);
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

        try {
            MindmapsGraph graph = GraphFactory.getInstance().getGraph(currentGraphName);

            LOG.debug("Start querying for: [{}]", req.queryParams(REST.Request.QUERY_FIELD));
            MatchQuery matchQuery = withGraph(graph).parse(req.queryParams(REST.Request.QUERY_FIELD));
            Collection<Map<String, Concept>> graqlResultsList = matchQuery
                    .limit(SAFETY_LIMIT)
                    .stream().collect(Collectors.toList());
            LOG.debug("Done querying.");

            Map<String, Collection<String>> linkedNodes = computeLinkedNodesFromQuery(matchQuery);
            Set<String> typesAskedInQuery = matchQuery.admin().getTypes().stream().map(Concept::getId).collect(Collectors.toSet());
            JSONArray halArray = buildHALRepresentations(graqlResultsList, linkedNodes, typesAskedInQuery);

            LOG.debug("Done building resources.");
            return halArray.toString();
        } catch (Exception e) {
            throw new MindmapsEngineServerException(500, e);
        }
    }

    private JSONArray buildHALRepresentations(Collection<Map<String, Concept>> graqlResultsList, Map<String, Collection<String>> linkedNodes, Set<String> typesAskedInQuery) {
        final JSONArray lines = new JSONArray();
        graqlResultsList.parallelStream()
                .forEach(resultLine -> resultLine.entrySet().forEach(current -> {
                    LOG.trace("Building HAL resource for concept with id {}", current.getValue().getId());
                    Representation currentHal = new HALConcept(current.getValue(), MATCH_QUERY_FIXED_DEGREE, true,
                            typesAskedInQuery).getRepresentation();
                    if (linkedNodes.containsKey(current.getKey()))
                        linkedNodes.get(current.getKey()).forEach(varName -> currentHal.withLink("edge_to", REST.WebPath.CONCEPT_BY_ID_URI + resultLine.get(varName).getId()));
                    lines.put(new JSONObject(currentHal.toString(RepresentationFactory.HAL_JSON)));
                }));
        return lines;
    }

    private Map<String, Collection<String>> computeLinkedNodesFromQuery(MatchQuery matchQuery) {
        final Map<String, Collection<String>> linkedNodes = new HashMap<>();
        matchQuery.admin().getPattern().getVars().forEach(var -> {
            //if in the current var is expressed some kind of relation (e.g. ($x,$y))
            if (var.getProperty(RelationProperty.class).isPresent()) {
                //collect all the role players in the current var's relations (e.g. 'x' and 'y')
                final List<String> rolePlayersInVar = var.getProperty(RelationProperty.class).get()
                        .getCastings().map(x -> x.getRolePlayer().getName()).collect(Collectors.toList());
                //if it is a binary or ternary relation
                if (rolePlayersInVar.size() > 1) {
                    rolePlayersInVar.forEach(rolePlayer -> {
                        linkedNodes.putIfAbsent(rolePlayer, new HashSet<>());
                        rolePlayersInVar.forEach(y -> {
                            if (!y.equals(rolePlayer))
                                linkedNodes.get(rolePlayer).add(y);
                        });
                    });
                }
            }
        });
        return linkedNodes;
    }

}
