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
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.factory.EngineGraknGraphFactory;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;
import java.util.Optional;

import static ai.grakn.GraknTxType.READ;
import static ai.grakn.engine.controller.GraqlController.getAcceptType;
import static ai.grakn.engine.controller.GraqlController.mandatoryQueryParameter;
import static ai.grakn.engine.controller.GraqlController.queryParameter;
import static ai.grakn.graql.internal.hal.HALBuilder.renderHALConceptData;
import static ai.grakn.util.ErrorMessage.MISSING_MANDATORY_PARAMETERS;
import static ai.grakn.util.ErrorMessage.NO_CONCEPT_IN_KEYSPACE;
import static ai.grakn.util.ErrorMessage.UNSUPPORTED_CONTENT_TYPE;
import static ai.grakn.util.REST.Request.Concept.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Concept.OFFSET_EMBEDDED;
import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.Graql.IDENTIFIER;
import static ai.grakn.util.REST.Response.Graql.RESPONSE;
import static ai.grakn.util.REST.Response.Json.ENTITIES_JSON_FIELD;
import static ai.grakn.util.REST.Response.Json.RELATIONS_JSON_FIELD;
import static ai.grakn.util.REST.Response.Json.RESOURCES_JSON_FIELD;
import static ai.grakn.util.REST.Response.Json.ROLES_JSON_FIELD;
import static ai.grakn.util.REST.WebPath.Concept.CONCEPT;
import static ai.grakn.util.REST.WebPath.Concept.ONTOLOGY;
import static java.util.stream.Collectors.toList;

/**
 * <p>
 *     Endpoints used to query the graph by concept type or identifier
 * </p>
 *
 * @author alexandraorth
 */
@Path("/graph")
public class ConceptController {

    private static final int separationDegree = 1;
    private final EngineGraknGraphFactory factory;

    public ConceptController(EngineGraknGraphFactory factory, Service spark){
        this.factory = factory;

        spark.get(CONCEPT + ID_PARAMETER,  this::conceptByIdentifier);
        spark.get(ONTOLOGY,  this::ontology);

    }

    @GET
    @Path("concept/{id}")
    @ApiOperation(
            value = "Return the HAL representation of a given concept.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = IDENTIFIER,      value = "Identifier of the concept", required = true, dataType = "string", paramType = "path"),
            @ApiImplicitParam(name = KEYSPACE,        value = "Name of graph to use", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = OFFSET_EMBEDDED, value = "Offset to begin at for embedded HAL concepts", required = true, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = LIMIT_EMBEDDED,  value = "Limit on the number of embedded HAL concepts", required = true, dataType = "boolean", paramType = "query")
    })
    private Json conceptByIdentifier(Request request, Response response){
        validateRequest(request);

        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        ConceptId conceptId = ConceptId.of(mandatoryRequestParameter(request, ID_PARAMETER));
        int offset = queryParameter(request, OFFSET_EMBEDDED).map(Integer::parseInt).orElse(0);
        int limit = queryParameter(request, LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);

        try(GraknGraph graph = factory.getGraph(keyspace, READ)){
            Json body = Json.object();
            Concept concept = retrieveExistingConcept(graph, conceptId);

            response.type(APPLICATION_HAL);
            response.status(200);

            body.set(RESPONSE,renderHALConceptData(concept, separationDegree, keyspace, offset, limit));

            return body;
        }
    }

    @GET
    @Path("/ontology")
    @ApiOperation(
            value = "Produces a Json object containing meta-ontology types instances.",
            notes = "The built Json object will contain ontology nodes divided in roles, entities, relations and resources.",
            response = Json.class)
    @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query")
    private String ontology(Request request, Response response) {
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);

        try(GraknGraph graph = factory.getGraph(keyspace, READ)){
            Json responseObj = Json.object();
            responseObj.set(ROLES_JSON_FIELD, instances(graph.admin().getMetaRoleType()));
            responseObj.set(ENTITIES_JSON_FIELD, instances(graph.admin().getMetaEntityType()));
            responseObj.set(RELATIONS_JSON_FIELD, instances(graph.admin().getMetaRelationType()));
            responseObj.set(RESOURCES_JSON_FIELD, instances(graph.admin().getMetaResourceType()));
            return responseObj.toString();
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    static Concept retrieveExistingConcept(GraknGraph graph, ConceptId conceptId){
        Concept concept = graph.getConcept(conceptId);

        if (notPresent(concept)) {
            throw new GraknEngineServerException(500, NO_CONCEPT_IN_KEYSPACE.getMessage(conceptId, graph.getKeyspace()));
        }

        return concept;
    }

    static void validateRequest(Request request){
        String acceptType = getAcceptType(request);

        if(!acceptType.equals(APPLICATION_HAL)){
            throw new GraknEngineServerException(406, UNSUPPORTED_CONTENT_TYPE, acceptType);
        }
    }

    private List<String> instances(Type type) {
        return type.subTypes().stream().map(Type::getLabel).map(TypeLabel::getValue).collect(toList());
    }

    /**
     * Given a {@link Request} object retrieve the value of the {@param parameter} argument. If it is not present
     * in the request URL, return a 404 to the client.
     *
     * @param request information about the HTTP request
     * @param parameter value to retrieve from the HTTP request
     * @return value of the given parameter
     */
    static String mandatoryRequestParameter(Request request, String parameter){
        return Optional.ofNullable(request.params(parameter)).orElseThrow(() ->
                new GraknEngineServerException(400, MISSING_MANDATORY_PARAMETERS, parameter));
    }

    /**
     * Check if the concept is a valid concept
     * @param concept the concept to validate
     * @return true if the concept is valid, false otherwise
     */
    private static boolean notPresent(Concept concept){
        return concept == null;
    }

}
