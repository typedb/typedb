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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.engine.util.ConfigProperties;
import ai.grakn.engine.visualiser.HALConceptRepresentationBuilder;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.factory.GraphFactory;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Reasoner;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.util.REST;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static spark.Spark.get;


@Path("/graph")
@Api(value = "/graph", description = "Endpoints used to query the graph by ID or Graql match query and build HAL objects.")
@Produces({"application/json", "text/plain"})
public class VisualiserController {

    private final Logger LOG = LoggerFactory.getLogger(VisualiserController.class);

    private String defaultGraphName;
    private int separationDegree;
    private final static String SHORTEST_PATH_QUERY = "path";
    private final static String COMPUTE_RESPONSE_TYPE = "type";
    private final static String COMPUTE_RESPONSE_FIELD = "response";

    //TODO: implement a pagination system.

    public VisualiserController() {

        defaultGraphName = ConfigProperties.getInstance().getProperty(ConfigProperties.DEFAULT_GRAPH_NAME_PROPERTY);
        separationDegree = ConfigProperties.getInstance().getPropertyAsInt(ConfigProperties.HAL_DEGREE_PROPERTY);

        get(REST.WebPath.CONCEPT_BY_ID_URI + REST.Request.ID_PARAMETER, this::getConceptById);
        get(REST.WebPath.CONCEPT_BY_ID_ONTOLOGY_URI + REST.Request.ID_PARAMETER, this::getConceptByIdOntology);

        get(REST.WebPath.GRAPH_MATCH_QUERY_URI, this::matchQuery);
        get(REST.WebPath.GRAPH_ANALYTICS_QUERY_URI, this::computeQuery);
        get(REST.WebPath.GRAPH_PRE_MATERIALISE_QUERY_URI, this::preMaterialiseAll);

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
        String graphNameParam = req.queryParams(REST.Request.GRAPH_NAME_PARAM);
        String currentGraphName = (graphNameParam == null) ? defaultGraphName : graphNameParam;

        try (GraknGraph graph = GraphFactory.getInstance().getGraph(currentGraphName)) {
            Concept concept = graph.getConcept(req.params(REST.Request.ID_PARAMETER));
            LOG.trace("Building HAL resource for concept with id {}", concept.getId());
            return HALConceptRepresentationBuilder.renderHALConceptData(concept, separationDegree);

        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
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
        String graphNameParam = req.queryParams(REST.Request.GRAPH_NAME_PARAM);
        String currentGraphName = (graphNameParam == null) ? defaultGraphName : graphNameParam;

        try (GraknGraph graph = GraphFactory.getInstance().getGraph(currentGraphName)) {
            Concept concept = graph.getConcept(req.params(REST.Request.ID_PARAMETER));
            LOG.trace("Building HAL resource for concept with id {}", concept.getId());
            return HALConceptRepresentationBuilder.renderHALConceptOntology(concept);

        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    @GET
    @Path("/match")
    @ApiOperation(
            value = "Executes match query on the server and build HAL representation for each concept in the query result.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "graphName", value = "Name of graph to use", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "query", value = "Match query to execute", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reasoner", value = "Boolean used to decide whether run reasoner together with the current query.", required = true, dataType = "sting/boolean", paramType = "query")

    })
    private String matchQuery(Request req, Response res) {

        String currentGraphName = req.queryParams(REST.Request.GRAPH_NAME_PARAM);
        if (currentGraphName == null) currentGraphName = defaultGraphName;

        boolean useReasoner = Boolean.parseBoolean(req.queryParams("reasoner"));

        try (GraknGraph graph = GraphFactory.getInstance().getGraph(currentGraphName)) {

            LOG.debug("Start querying for: [{}]", req.queryParams(REST.Request.QUERY_FIELD));
            MatchQuery matchQuery = graph.graql().parse(req.queryParams(REST.Request.QUERY_FIELD));

            MatchQuery currentMatchQuery;

            if (useReasoner) {
                currentMatchQuery = new Reasoner(graph).resolveToQuery(matchQuery, true);
                graph.commit();
            } else {
                currentMatchQuery = matchQuery;
            }

            Collection<Map<String, Concept>> graqlResultsList = currentMatchQuery
                    .stream()
                    .collect(Collectors.toList());
            graph.commit();
            LOG.debug("Done querying, " + graqlResultsList.size() + " results found");

            JSONArray halArray = HALConceptRepresentationBuilder.renderHALArrayData(matchQuery, graqlResultsList);
            LOG.debug("Done building resources.");

            return halArray.toString();
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    @GET
    @Path("/analytics")
    @ApiOperation(
            value = "Executes compute query on the server and build HAL representation of result or returns string containing statistics.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "graphName", value = "Name of graph to use", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "query", value = "Compute query to execute", required = true, dataType = "string", paramType = "query")
    })
    private String computeQuery(Request req, Response res) {

        String currentGraphName = req.queryParams(REST.Request.GRAPH_NAME_PARAM);
        if (currentGraphName == null) currentGraphName = defaultGraphName;

        try (GraknGraph graph = GraphFactory.getInstance().getGraph(currentGraphName)) {

            String query = req.queryParams(REST.Request.QUERY_FIELD);
            LOG.debug("Start querying for: [{}]", query);
            JSONObject responseObject = new JSONObject();
            ComputeQuery computeQuery = graph.graql().parse(req.queryParams(REST.Request.QUERY_FIELD));

            if (query.contains(SHORTEST_PATH_QUERY)) {
                responseObject.put(COMPUTE_RESPONSE_TYPE, "HAL");
                JSONArray array = new JSONArray();
                ((List<Concept>)computeQuery.execute()).iterator().forEachRemaining(concept ->
                       array.put(HALConceptRepresentationBuilder.renderHALConceptData(concept, 0)));
                responseObject.put(COMPUTE_RESPONSE_FIELD,array);
            } else {
                responseObject.put(COMPUTE_RESPONSE_TYPE, "string");
                responseObject.put(COMPUTE_RESPONSE_FIELD,
                        computeQuery.resultsString(Printers.graql())
                                .map(x -> x.replaceAll("\u001B\\[\\d+[m]", ""))
                                .collect(Collectors.joining())
                );
            }
            return responseObject.toString();
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    @GET
    @Path("/preMaterialiseAll")
    @ApiOperation(
            value = "Pre materialise all the rules on the graph.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "graphName", value = "Name of graph to use", dataType = "string", paramType = "query"),
    })
    private String preMaterialiseAll(Request req, Response res) {

        String currentGraphName = req.queryParams(REST.Request.GRAPH_NAME_PARAM);
        if (currentGraphName == null) currentGraphName = defaultGraphName;

        try (GraknGraph graph = GraphFactory.getInstance().getGraph(currentGraphName)) {
            new Reasoner(graph).precomputeInferences();
            return "Done.";
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

}
