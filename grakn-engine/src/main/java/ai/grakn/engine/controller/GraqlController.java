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
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.exception.GraknServerException;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.exception.InvalidGraphException;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.analytics.PathQuery;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.util.REST;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import static ai.grakn.GraknTxType.WRITE;
import static ai.grakn.engine.controller.util.Requests.mandatoryBody;
import static ai.grakn.engine.controller.util.Requests.mandatoryQueryParameter;
import static ai.grakn.engine.controller.util.Requests.queryParameter;
import static ai.grakn.util.REST.Request.Graql.INFER;
import static ai.grakn.util.REST.Request.Graql.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Graql.MATERIALISE;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON_GRAQL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_TEXT;
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

        spark.post(REST.WebPath.Graph.ANY_GRAQL, this::executeGraql);
        spark.get(REST.WebPath.Graph.GRAQL,    this::executeGraqlGET);

        //TODO The below exceptions are very broad. They should be revised after we improve exception
        //TODO hierarchies in Graql and Graph
        spark.exception(GraqlQueryException.class, (e, req, res) -> handleError(400, e, res));
        spark.exception(GraqlSyntaxException.class, (e, req, res) -> handleError(400, e, res));

        // Handle invalid type castings and invalid insertions
        spark.exception(GraphOperationException.class, (e, req, res) -> handleError(422, e, res));
        spark.exception(InvalidGraphException.class, (e, req, res) -> handleError(422, e, res));
    }

    @POST
    @Path("/execute")
    @ApiOperation(value = "Execute an arbitrary Graql queryEndpoints used to query the graph by ID or Graql match query and build HAL objects.")
    private Object executeGraql(Request request, Response response) {
        String queryString = mandatoryBody(request);
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        boolean infer = parseBoolean(mandatoryQueryParameter(request, INFER));
        boolean materialise = parseBoolean(mandatoryQueryParameter(request, MATERIALISE));
        int limitEmbedded = queryParameter(request, LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);
        String acceptType = getAcceptType(request);

        try(GraknGraph graph = factory.getGraph(keyspace, WRITE)){
            Query<?> query = graph.graql().materialise(materialise).infer(infer).parse(queryString);
            Object resp = respond(response, acceptType, executeQuery(keyspace, limitEmbedded, query, acceptType));
            graph.commit();
            return resp;
        }
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
    private Object executeGraqlGET(Request request, Response response) {
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        String queryString = mandatoryQueryParameter(request, QUERY);
        boolean infer = parseBoolean(mandatoryQueryParameter(request, INFER));
        boolean materialise = parseBoolean(mandatoryQueryParameter(request, MATERIALISE));
        int limitEmbedded = queryParameter(request, LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);
        String acceptType = getAcceptType(request);

        try(GraknGraph graph = factory.getGraph(keyspace, WRITE)){
            Query<?> query = graph.graql().materialise(materialise).infer(infer).parse(queryString);

            if(!query.isReadOnly()) throw GraknServerException.invalidQuery("\"read-only\"");

            if(!validContentType(acceptType, query)) throw GraknServerException.contentTypeQueryMismatch(acceptType, query);

            Json responseBody = executeQuery(keyspace, limitEmbedded, query, acceptType);
            return respond(response, acceptType, responseBody);
        }
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
     * @param contentType content type being provided in the response
     * @param response response to the client
     * @return formatted result of the executed query
     */
    private Object respond(Response response, String contentType, Object responseBody){
        response.type(contentType);
        response.body(responseBody.toString());
        response.status(200);

        return responseBody;
    }

    /**
     * Execute a query and return a response in the format specified by the request.
     *
     * @param keyspace the keyspace the query is running on
     * @param query read query to be executed
     * @param acceptType response format that the client will accept
     */
    private Json executeQuery(String keyspace, int limitEmbedded, Query<?> query, String acceptType){
        Printer<?> printer;

        switch (acceptType) {
            case APPLICATION_TEXT:
                printer = Printers.graql(false);
                break;
            case APPLICATION_JSON_GRAQL:
                printer = Printers.json();
                break;
            case APPLICATION_HAL:
                printer = Printers.hal(keyspace, limitEmbedded);
                break;
            default:
                throw GraknServerException.unsupportedContentType(acceptType);
        }

        String formatted = printer.graqlString(query.execute());

        Object response = acceptType.equals(APPLICATION_TEXT) ? formatted : Json.read(formatted);

        return Json.object(RESPONSE, response);
    }

    static String getAcceptType(Request request) {
        // TODO - we are not handling multiple values here and we should!
        String header = request.headers("Accept");
        return header == null ? "" : request.headers("Accept").split(",")[0];
    }
}
