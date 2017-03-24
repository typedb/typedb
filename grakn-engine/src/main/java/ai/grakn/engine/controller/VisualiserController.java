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
import ai.grakn.GraknTxType;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.util.REST;
import io.swagger.annotations.Api;
import mjson.Json;
import org.apache.http.entity.ContentType;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.Optional;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.hal.HALConceptRepresentationBuilder.renderHALArrayData;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_PARAMETERS;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static java.lang.Boolean.parseBoolean;

/**
 * <p>
 * Endpoints used to query the graph by ID or Graql match query and build a HAL or Graql response.
 * </p>
 *
 * @author Marco Scoppetta, alexandraorth
 */
@Path("/graph")
@Api(value = "/graph", description = "Endpoints used to query the graph by ID or Graql match query and build HAL objects.")
@Produces({"application/json", "text/plain"})
public class VisualiserController {

    private final EngineGraknGraphFactory factory;

    public VisualiserController(EngineGraknGraphFactory factory, Service spark) {
        this.factory = factory;

        spark.get(REST.WebPath.Graph.GRAQL, this::executeGraql);

        spark.exception(IllegalArgumentException.class, (e, req, res) -> handleGraqlSyntaxError(e, res));
    }

    @GET
    private Json executeGraql(Request request, Response response){
        String keyspace = getMandatoryParameter(request, "keyspace");
        String queryString = getMandatoryParameter(request, "query");
        boolean infer = parseBoolean(getMandatoryParameter(request, "infer"));
        boolean materialise = parseBoolean(getMandatoryParameter(request, "materialise"));
        int limit = validLimit(getParameter(request, "limit"));

        try(GraknGraph graph = factory.getGraph(keyspace, GraknTxType.READ)){
            Query<?> query = graph.graql().materialise(materialise).infer(infer).parse(queryString);

            if(!readOnly(query)){
                throw new IllegalArgumentException("Only \"read-only\" queries are allowed.");
            }

            return respond(request, query, response, limit);
        }
    }

    /**
     * Given a {@link Request} object retrieve the value of the {@param parameter} argument. If it is not present
     * in the request, return a 404 to the client.
     *
     * @param request information about the HTTP request
     * @param parameter value to retrieve from the HTTP request
     * @return value of the given parameter
     */
    //TODO Merge this with the one in TasksController
    private String getMandatoryParameter(Request request, String parameter){
        return getParameter(request, parameter).orElseThrow(() ->
                new GraknEngineServerException(400, MISSING_MANDATORY_PARAMETERS, parameter));
    }

    /**
     * Given a {@link Request}, retrieve the value of the {@param parameter}
     * @param request information about the HTTP request
     * @param parameter value to retrieve from the HTTP request
     * @return value of the given parameter
     */
    private Optional<String> getParameter(Request request, String parameter){
        return Optional.ofNullable(request.queryParams(parameter));
    }

    /**
     * Handle any {@link IllegalArgumentException} that are thrown by the server. Configures and returns
     * the correct JSON response.
     *
     * @param exception exception thrown by the server
     * @param response response to the client
     */
    private static void handleGraqlSyntaxError(Exception exception, Response response){
        response.status(400);
        response.body(Json.object("exception", exception.getMessage()).toString());
        response.type(ContentType.APPLICATION_JSON.getMimeType());
    }

    /**
     * This API only supports read-only queries, or non-insert queries.
     * @param query the query to check
     * @return if the query is read-only
     */
    private boolean readOnly(Query<?> query){
        return !(query instanceof InsertQuery);
    }

    /**
     * Validates a parameter as a valid limit
     * @param optionalLimit the limit argument to validate
     * @return the valid limit
     */
    private int validLimit(Optional<String> optionalLimit){
        int limit = optionalLimit.map(Integer::parseInt).orElse(Integer.MAX_VALUE);
        if(limit < 0){
            throw new IllegalArgumentException("Invalid limit");
        }
        return limit;
    }

    /**
     * Format the response with the correct content type based on the request.
     *
     * @param request
     * @param query
     * @param response
     * @param limit
     * @return
     */
    private Json respond(Request request, Query<?> query, Response response, int limit){

        Json body = Json.object("originalQuery", query.toString());

        switch (getAcceptType(request)){
            case APPLICATION_TEXT:
                response.type(APPLICATION_TEXT);
                body.set("response", formatAsGraql(query, limit));
                break;
            case APPLICATION_HAL:
                response.type(APPLICATION_HAL);
                body.set("response", formatAsHAL(query, limit));
                break;
            default:
                throw new GraknEngineServerException(406, "Unsupported Content-Type requested.");
        }

        response.body(body.toString());
        response.status(200);

        return body;
    }

//    @GET
//    @Path("/concept/:uuid")
//    @ApiOperation(
//            value = "Return the HAL representation of a given concept.")
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = "id", value = "ID of the concept", required = true, dataType = "string", paramType = "path"),
//            @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query")
//    })
//    private String conceptById(Request req, Response res) {
//        String keyspace = getKeyspace(req);
//
//        try (GraknGraph graph = getInstance().getGraph(keyspace)) {
//
//            int offset = (req.queryParams().contains("offset")) ? Integer.parseInt(req.queryParams("offset")) : 0;
//            int limit =  (req.queryParams().contains("limit")) ? Integer.parseInt(req.queryParams("limit")) : -1;
//
//            Concept concept = graph.getConcept(ConceptId.of(req.params(ID_PARAMETER)));
//
//            if (concept == null) {
//                throw new GraknEngineServerException(500, ErrorMessage.NO_CONCEPT_IN_KEYSPACE.getMessage(req.params(ID_PARAMETER), keyspace));
//            }
//
//            return renderHALConceptData(concept, separationDegree, keyspace, offset, limit);
//        } catch (RuntimeException e) {
//            throw new GraknEngineServerException(500, e);
//        }
//    }
//
//    @GET
//    @Path("/concept/ontology/:uuid")
//    @ApiOperation(
//            value = "Return the HAL representation of a given concept.")
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = "id", value = "ID of the concept", required = true, dataType = "string", paramType = "path"),
//            @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query")
//    })
//    private String conceptByIdOntology(Request req, Response res) {
//        String keyspace = getKeyspace(req);
//
//        try (GraknGraph graph = getInstance().getGraph(keyspace)) {
//            Concept concept = graph.getConcept(ConceptId.of(req.params(ID_PARAMETER)));
//            return renderHALConceptOntology(concept, keyspace);
//        } catch (RuntimeException e) {
//            throw new GraknEngineServerException(500, e);
//        }
//    }
//
//    @GET
//    @Path("/ontology")
//    @ApiOperation(
//            value = "Produces a JSONObject containing meta-ontology types instances.",
//            notes = "The built JSONObject will contain ontology nodes divided in roles, entities, relations and resources.",
//            response = JSONObject.class)
//    @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query")
//    private String ontology(Request req, Response res) {
//        String keyspace = getKeyspace(req);
//
//        try (GraknGraph graph = getInstance().getGraph(keyspace)) {
//            JSONObject responseObj = new JSONObject();
//            responseObj.put(ROLES_JSON_FIELD, instances(graph.admin().getMetaRoleType()));
//            responseObj.put(ENTITIES_JSON_FIELD, instances(graph.admin().getMetaEntityType()));
//            responseObj.put(RELATIONS_JSON_FIELD, instances(graph.admin().getMetaRelationType()));
//            responseObj.put(RESOURCES_JSON_FIELD, instances(graph.admin().getMetaResourceType()));
//            return responseObj.toString();
//        } catch (RuntimeException e) {
//            throw new GraknEngineServerException(500, e);
//        }
//    }
//
//    @GET
//    @Path("/match")
//    @ApiOperation(
//            value = "Executes match query on the server and build a representation for each concept in the query result. " +
//                    "Return type is determined by the content type. Either application/graql or application/json/hal")
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "query", value = "Match query to execute", required = true, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reasoner", value = "Boolean used to decide whether run reasoner together with the current query.", required = true, dataType = "sting/boolean", paramType = "query")
//    })
//    private String match(Request req, Response res) {
//        String keyspace = getKeyspace(req);
//        boolean useReasoner = parseBoolean(req.queryParams("reasoner"));
//        boolean materialise = parseBoolean(req.queryParams("materialise"));
//
//        try (GraknGraph graph = getInstance().getGraph(keyspace)) {
//            QueryBuilder qb = graph.graql().infer(useReasoner).materialise(materialise);
//            Query parsedQuery = qb.parse(req.queryParams(QUERY_FIELD));
//            int limit = (req.queryParams().contains("limit")) ? Integer.parseInt(req.queryParams("limit")) : -1;
//
//            if (parsedQuery instanceof MatchQuery || parsedQuery instanceof AggregateQuery || parsedQuery instanceof ComputeQuery) {
//                switch (getAcceptType(req)) {
//                    case HAL_CONTENTTYPE:
//                        return formatAsHAL((MatchQuery) parsedQuery, keyspace, limit);
//                    case GRAQL_CONTENTTYPE:
//                        return formatAsGraql(parsedQuery);
//                    default:
//                        return formatAsHAL((MatchQuery) parsedQuery, keyspace, limit);
//                }
//            } else {
//                throw new GraknEngineServerException(500, "Only \"read-only\" queries are allowed from Grakn web-dashboard.");
//            }
//        } catch (IllegalArgumentException | IllegalStateException e) {
//            throw new GraknEngineServerException(HttpStatus.BAD_REQUEST_400, e.getMessage());
//        } catch (RuntimeException e) {
//            throw new GraknEngineServerException(500, e);
//        }
//    }
//
//    @GET
//    @Path("/analytics")
//    @ApiOperation(
//            value = "Executes compute query on the server and build HAL representation of result or returns string containing statistics.")
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "query", value = "Compute query to execute", required = true, dataType = "string", paramType = "query")
//    })
//    private String compute(Request req, Response res) {
//        try (GraknGraph graph = getInstance().getGraph(getKeyspace(req))) {
//
//            ComputeQuery computeQuery = graph.graql().parse(req.queryParams(QUERY_FIELD));
//            JSONObject response = new JSONObject();
//            if (computeQuery instanceof PathQuery) {
//                PathQuery pathQuery = (PathQuery) computeQuery;
//
//                Optional<List<Concept>> result = pathQuery.execute();
//
//                if (result.isPresent()) {
//                    response.put(COMPUTE_RESPONSE_TYPE, "HAL");
//                    JSONArray array = new JSONArray();
//                    result.get().forEach(concept ->
//                            array.put(renderHALConceptData(concept, 0, getKeyspace(req), 0, 100))
//                    );
//                    response.put(COMPUTE_RESPONSE_FIELD, array);
//                } else {
//                    response.put(COMPUTE_RESPONSE_TYPE, "string");
//                    response.put(COMPUTE_RESPONSE_FIELD, "No path found");
//                }
//            } else {
//                response.put(COMPUTE_RESPONSE_TYPE, "string");
//                response.put(COMPUTE_RESPONSE_FIELD, formatAsGraql(computeQuery));
//            }
//            return response.toString();
//        } catch (RuntimeException e) {
//            throw new GraknEngineServerException(500, e);
//        }
//    }
//
//    @GET
//    @Path("/preMaterialiseAll")
//    @ApiOperation(value = "Pre materialise all the rules on the graph.")
//    @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query")
//    private String preMaterialiseAll(Request req, Response res) {
//        try (GraknGraph graph = getInstance().getGraph(getKeyspace(req))) {
//            //TODO: Fix ugly casting here
//            Reasoner.precomputeInferences(graph);
//            return "Done.";
//        } catch (RuntimeException e) {
//            throw new GraknEngineServerException(500, e);
//        }
//    }
//
    /**
     * Format a match query as HAL
     *
     * @param query query to format
     * @return HAL representation
     */
    private Json formatAsHAL(Query<?> query, int limit) {
        return renderHALArrayData((MatchQuery) query, 0, limit);
    }

    /**
     * Format query results as Graql
     *
     * @param query query to format
     * @return Graql representation
     */
    private String formatAsGraql(Query<?> query, int limit) {
        return query.resultsString(Printers.graql())
                .map(x -> x.replaceAll("\u001B\\[\\d+[m]", ""))
                .limit(limit)
                .collect(Collectors.joining("\n"));
    }

    private static String getAcceptType(Request request){
        return request.headers("Accept").split(",")[0];
    }
}
