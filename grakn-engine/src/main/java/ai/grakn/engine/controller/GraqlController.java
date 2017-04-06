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
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.util.REST;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import java.util.ArrayList;
import mjson.Json;
import org.apache.http.entity.ContentType;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.Optional;

import static ai.grakn.GraknTxType.READ;
import static ai.grakn.graql.internal.hal.HALBuilder.renderHALConceptData;
import static ai.grakn.graql.internal.hal.HALBuilder.renderHALArrayData;
import static ai.grakn.util.ErrorMessage.INVALID_CONTENT_TYPE;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_PARAMETERS;
import static ai.grakn.util.ErrorMessage.UNSUPPORTED_CONTENT_TYPE;
import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Request.Graql.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Graql.MATERIALISE;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.Graql.ORIGINAL_QUERY;
import static ai.grakn.util.REST.Response.Graql.RESPONSE;
import static java.lang.Boolean.parseBoolean;

/**
 * <p>
 * Endpoints used to query the graph using Graql and build a HAL, Graql or Json response.
 * </p>
 *
 * @author Marco Scoppetta, alexandraorth
 */
@Path("/graph/graql")
@Api(value = "/graph/graql", description = "Endpoints used to query the graph by ID or Graql match query and build HAL objects.")
@Produces({"application/json", "text/plain"})
public class GraqlController {

    private final EngineGraknGraphFactory factory;

    public GraqlController(EngineGraknGraphFactory factory, Service spark) {
        this.factory = factory;

        spark.get(REST.WebPath.Graph.GRAQL, this::executeGraql);

        spark.exception(IllegalArgumentException.class, (e, req, res) -> handleGraqlSyntaxError(e, res));
    }

    @GET
    @Path("/")
    @ApiOperation(
            value = "Executes graql query on the server and build a representation for each concept in the query result. " +
                    "Return type is determined by the provided accept type: application/graql+json, application/hal+json or application/text")
    @ApiImplicitParams({
            @ApiImplicitParam(name = KEYSPACE,    value = "Name of graph to use", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QUERY,       value = "Match query to execute", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = INFER,       value = "Should reasoner with the current query.", required = true, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = MATERIALISE, value = "Should reasoner materialise results with the current query.", required = true, dataType = "boolean", paramType = "query")
    })
    private Json executeGraql(Request request, Response response){
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        String queryString = mandatoryQueryParameter(request, QUERY);
        boolean infer = parseBoolean(mandatoryQueryParameter(request, INFER));
        boolean materialise = parseBoolean(mandatoryQueryParameter(request, MATERIALISE));

        try(GraknGraph graph = factory.getGraph(keyspace, READ)){
            Query<?> query = graph.graql().materialise(materialise).infer(infer).parse(queryString);

            if(!readOnly(query)){
                throw new GraknEngineServerException(405, "Only \"read-only\" queries are allowed.");
            }

            return respond(request, query, response);
        }
    }

    /**
     * Given a {@link Request} object retrieve the value of the {@param parameter} argument. If it is not present
     * in the request query, return a 404 to the client.
     *
     * @param request information about the HTTP request
     * @param parameter value to retrieve from the HTTP request
     * @return value of the given parameter
     */
    static String mandatoryQueryParameter(Request request, String parameter){
        return queryParameter(request, parameter).orElseThrow(() ->
                new GraknEngineServerException(400, MISSING_MANDATORY_PARAMETERS, parameter));
    }

    /**
     * Given a {@link Request}, retrieve the value of the {@param parameter}
     * @param request information about the HTTP request
     * @param parameter value to retrieve from the HTTP request
     * @return value of the given parameter
     */
    static Optional<String> queryParameter(Request request, String parameter){
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
     * Check if the supported combinations of query type and content type are true
     * @param acceptType provided accept type of the request
     * @param query provided query from the request
     * @return if the combination of query and accept type is valid
     */
    private boolean validContentType(String acceptType, Query<?> query){

        // If compute other than path and not TEXT invalid
        if (query instanceof ComputeQuery && !(query instanceof PathQuery) && acceptType.equals(APPLICATION_HAL)){
            return false;
        }

        // If aggregate and HAL invalid
        else if(query instanceof AggregateQuery && acceptType.equals(APPLICATION_HAL)){
            return false;
        }

        return true;
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
     * Format the response with the correct content type based on the request.
     *
     * @param request information about the HTTP request
     * @param query query to be executed
     * @param response response to the client
     * @return formatted result of the executed query
     */
    private Json respond(Request request, Query<?> query, Response response){

        Json body = Json.object(ORIGINAL_QUERY, query.toString());

        String acceptType = getAcceptType(request);
        if(!validContentType(acceptType, query)){
            throw new GraknEngineServerException(406, INVALID_CONTENT_TYPE, query.getClass().getName(), acceptType);
        }

        switch (acceptType){
            case APPLICATION_TEXT:
                body.set(RESPONSE, formatAsGraql(Printers.graql(), query));
                break;
            case APPLICATION_JSON_GRAQL:
                body.set(RESPONSE, Json.read(formatAsGraql(Printers.json(), query)));
                break;
            case APPLICATION_HAL:
                // Extract extra information needed by HAL renderer
                String keyspace = mandatoryQueryParameter(request, KEYSPACE);
                int limitEmbedded = queryParameter(request, LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);

                body.set(RESPONSE, formatAsHAL(query, keyspace, limitEmbedded));
                break;
            default:
                throw new GraknEngineServerException(406, UNSUPPORTED_CONTENT_TYPE, acceptType);
        }

        response.type(acceptType);
        response.body(body.toString());
        response.status(200);

        return body;
    }

    /**
     * Format a match query as HAL
     *
     * @param query query to format
     * @param numberEmbeddedComponents the number of embedded components for the HAL format, taken from the request
     * @param keyspace the keyspace from the request //TODO only needed because HAL does not support admin interface
     * @return HAL representation
     */
    private Json formatAsHAL(Query<?> query, String keyspace, int numberEmbeddedComponents) {
        // This ugly instanceof business needs to be done because the HAL array renderer does not
        // support Compute queries and because Compute queries do not have the "admin" interface

        if(query instanceof MatchQuery) {
            return renderHALArrayData((MatchQuery) query, 0, numberEmbeddedComponents);
        } else if(query instanceof PathQuery) {
            Json array = Json.array();
            // The below was taken line-for-line from previous way of rendering
            ((PathQuery) query).execute()
                    .orElse(new ArrayList<>())
                    .forEach(c -> array.add(
                            renderHALConceptData(c, 0, keyspace, 0, numberEmbeddedComponents)));

            return array;
        }

        throw new RuntimeException("Unsupported query type in HAL formatter");
    }

    /**
     * Format query results as Graql based on the provided printer
     *
     * @param query query to format
     * @return Graql representation
     */
    private String formatAsGraql(Printer printer, Query<?> query) {
        //TODO Make the Graql printer take in a "should colorize" argument
        //TODO Frontend does not need to unescape

        String result = printer.graqlString( query.execute());

        // unescape result
        result = result.replaceAll("\u001B\\[\\d+m", "");

        return result;
    }

    static String getAcceptType(Request request){
        return request.headers("Accept").split(",")[0];
    }
}
