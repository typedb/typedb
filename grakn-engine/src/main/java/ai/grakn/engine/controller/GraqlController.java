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
import ai.grakn.concept.ConceptId;
import ai.grakn.exception.ConceptException;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.DeleteQuery;
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
import java.util.Collection;
import java.util.stream.Collectors;
import javax.ws.rs.POST;
import mjson.Json;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.ArrayList;
import java.util.Optional;

import static ai.grakn.GraknTxType.WRITE;
import static ai.grakn.graql.internal.hal.HALBuilder.renderHALArrayData;
import static ai.grakn.graql.internal.hal.HALBuilder.renderHALConceptData;
import static ai.grakn.util.ErrorMessage.INVALID_CONTENT_TYPE;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_REQUEST_PARAMETERS;
import static ai.grakn.util.ErrorMessage.MISSING_REQUEST_BODY;
import static ai.grakn.util.ErrorMessage.UNSUPPORTED_CONTENT_TYPE;
import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Request.Graql.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Graql.MATERIALISE;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
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

    private static final Logger LOG = LoggerFactory.getLogger(GraqlController.class);
    private final EngineGraknGraphFactory factory;

    public GraqlController(EngineGraknGraphFactory factory, Service spark) {
        this.factory = factory;

        spark.get(REST.WebPath.Graph.GRAQL,    this::executeGraqlGET);
        spark.post(REST.WebPath.Graph.GRAQL,   this::executeGraqlPOST);
        spark.delete(REST.WebPath.Graph.GRAQL, this::executeGraqlDELETE);

        //TODO The below exceptions are very broad. They should be revised after we improve exception
        //TODO hierarchies in Graql and Graph
        // Handle graql syntax exceptions
        spark.exception(IllegalStateException.class, (e, req, res) -> handleError(400, e, res));
        spark.exception(IllegalArgumentException.class, (e, req, res) -> handleError(400, e, res));

        // Handle invalid type castings and invalid insertions
        spark.exception(ConceptException.class, (e, req, res) -> handleError(422, e, res));
        spark.exception(GraknValidationException.class, (e, req, res) -> handleError(422, e, res));
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
    private Json executeGraqlGET(Request request, Response response){
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        String queryString = mandatoryQueryParameter(request, QUERY);
        boolean infer = parseBoolean(mandatoryQueryParameter(request, INFER));
        boolean materialise = parseBoolean(mandatoryQueryParameter(request, MATERIALISE));
        String acceptType = getAcceptType(request);

        try(GraknGraph graph = factory.getGraph(keyspace, WRITE)){
            Query<?> query = graph.graql().materialise(materialise).infer(infer).parse(queryString);

            if(!query.isReadOnly()){
                throw new GraknEngineServerException(405, "Only \"read-only\" queries are allowed.");
            }

            if(!validContentType(acceptType, query)){
                throw new GraknEngineServerException(406, INVALID_CONTENT_TYPE, query.getClass().getName(), acceptType);
            }

            Json responseBody = executeReadQuery(request, query, acceptType);
            return respond(response, query, acceptType, responseBody);
        }
    }

    @POST
    @Path("/")
    @ApiOperation(
            value = "Executes graql insert query on the server and returns the IDs of the inserted concepts.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = KEYSPACE,    value = "Name of graph to use", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QUERY,       value = "Insert query to execute", required = true, dataType = "string", paramType = "body"),
    })
    private Json executeGraqlPOST(Request request, Response response){
        String queryString = mandatoryBody(request);
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);

        try(GraknGraph graph = factory.getGraph(keyspace, WRITE)){
            Query<?> query = graph.graql().materialise(false).infer(false).parse(queryString);

            if(!(query instanceof InsertQuery)){
                throw new GraknEngineServerException(405, "Only INSERT queries are allowed.");
            }

            Json responseBody = executeInsertQuery((InsertQuery) query);

            // Persist the transaction results TODO This should use a within-engine commit
            graph.commit();

            return respond(response, query, APPLICATION_JSON, responseBody);
        }
    }

    @POST
    @Path("/")
    @ApiOperation(value = "Executes graql delete query on the server.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = KEYSPACE,    value = "Name of graph to use", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QUERY,       value = "Insert query to execute", required = true, dataType = "string", paramType = "body"),
    })
    private Json executeGraqlDELETE(Request request, Response response){
        String queryString = mandatoryBody(request);
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);

        try(GraknGraph graph = factory.getGraph(keyspace, WRITE)){
            Query<?> query = graph.graql().materialise(false).infer(false).parse(queryString);

            if(!(query instanceof DeleteQuery)){
                throw new GraknEngineServerException(405, "Only DELETE queries are allowed.");
            }

            // Execute the query
            ((DeleteQuery) query).execute();

            // Persist the transaction results TODO This should use a within-engine commit
            graph.commit();

            return respond(response, query, APPLICATION_JSON, Json.object());
        }
    }

    /**
     * Given a {@link Request} object retrieve the value of the {@param parameter} argument. If it is not present
     * in the request query, return a 400 to the client.
     *
     * @param request information about the HTTP request
     * @param parameter value to retrieve from the HTTP request
     * @return value of the given parameter
     */
    static String mandatoryQueryParameter(Request request, String parameter){
        return queryParameter(request, parameter).orElseThrow(() ->
                new GraknEngineServerException(400, MISSING_MANDATORY_REQUEST_PARAMETERS, parameter));
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
     * Given a {@link Request), retreive the value of the request body. If the request does not have a body,
     * return a 400 (missing parameter) to the client.
     *
     * @param request information about the HTTP request
     * @return value of the request body as a string
     */
    static String mandatoryBody(Request request){
        return Optional.ofNullable(request.body()).filter(s -> !s.isEmpty()).orElseThrow(() ->
                new GraknEngineServerException(400, MISSING_REQUEST_BODY));
    }

    /**
     * Handle any {@link Exception} that are thrown by the server. Configures and returns
     * the correct JSON response with the given status.
     *
     * @param exception exception thrown by the server
     * @param response response to the client
     */
    private static void handleError(int status, Exception exception, Response response){
        LOG.error("REST error", exception);
        response.status(status);
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
     * Format the response with the correct content type based on the request.
     *
     * @param query query to be executed
     * @param contentType content type being provided in the response
     * @param response response to the client
     * @return formatted result of the executed query
     */
    private Json respond(Response response, Query<?> query, String contentType, Json responseBody){
        responseBody.set(ORIGINAL_QUERY, query.toString());

        response.type(contentType);
        response.body(responseBody.toString());
        response.status(200);

        return responseBody;
    }

    /**
     * Execute an insert query on the server and return a Json object with the Ids of the inserted elements.
     *
     * @param query insert query to be executed
     */
    private Json executeInsertQuery(InsertQuery query){
        Collection<String> concepts = query.execute().stream()
                .flatMap(answer -> answer.values().stream())
                .map(Concept::getId)
                .map(ConceptId::getValue)
                .collect(Collectors.toList());

        return Json.object(RESPONSE, concepts);
    }

    /**
     * Execute a read query and return a response in the format specified by the request.
     *
     * @param request information about the HTTP request
     * @param query read query to be executed
     * @param acceptType response format that the client will accept
     */
    private Json executeReadQuery(Request request, Query<?> query, String acceptType){
        switch (acceptType){
            case APPLICATION_TEXT:
                return Json.object(RESPONSE, formatAsGraql(Printers.graql(false), query));
            case APPLICATION_JSON_GRAQL:
                return Json.object(RESPONSE, Json.read(formatAsGraql(Printers.json(), query)));
            case APPLICATION_HAL:
                // Extract extra information needed by HAL renderer
                String keyspace = mandatoryQueryParameter(request, KEYSPACE);
                int limitEmbedded = queryParameter(request, LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);

                return Json.object(RESPONSE, formatAsHAL(query, keyspace, limitEmbedded));
            default:
                throw new GraknEngineServerException(406, UNSUPPORTED_CONTENT_TYPE, acceptType);
        }

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
        return printer.graqlString(query.execute());
    }

    static String getAcceptType(Request request) {
        // TODO - we are not handling multiple values here and we should!
        String header = request.headers("Accept");
        return header == null ? "" : request.headers("Accept").split(",")[0];
    }
}
