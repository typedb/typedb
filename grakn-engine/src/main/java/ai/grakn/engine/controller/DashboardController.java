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
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Query;
import ai.grakn.graql.internal.reasoner.Reasoner;
import ai.grakn.util.REST;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import mjson.Json;
import spark.Request;
import spark.Response;
import spark.Service;

import static ai.grakn.GraknTxType.READ;
import static ai.grakn.engine.controller.ConceptController.mandatoryRequestParameter;
import static ai.grakn.engine.controller.ConceptController.retrieveExistingConcept;
import static ai.grakn.engine.controller.ConceptController.validateRequest;
import static ai.grakn.engine.controller.GraqlController.mandatoryQueryParameter;
import static ai.grakn.engine.controller.GraqlController.queryParameter;
import static ai.grakn.graql.internal.hal.HALBuilder.HALExploreConcept;
import static ai.grakn.graql.internal.hal.HALBuilder.explanationAnswersToHAL;
import static ai.grakn.util.ErrorMessage.EXPLAIN_ONLY_MATCH;
import static ai.grakn.util.REST.Request.Concept.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Concept.OFFSET_EMBEDDED;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.Graql.IDENTIFIER;
import static ai.grakn.util.REST.Response.Graql.ORIGINAL_QUERY;
import static ai.grakn.util.REST.Response.Graql.RESPONSE;
import static java.util.stream.Collectors.toList;

/**
 * <p>
 *     Private endpoints used by dashboard to query by concept type.
 * </p>
 *
 * <p>
 *     This class should be thought of as a workplace/staging point for potential future user-facing endpoints.
 * </p>
 *
 * @author alexandraorth
 */
@Path("/dashboard")
public class DashboardController {

    private static final String RELATION_TYPES = REST.WebPath.Graph.GRAQL+"?query=match $a isa %s id '%s'; ($a,$b) isa %s; limit %s;&keyspace=%s&limitEmbedded=%s&infer=true&materialise=false";
    private static final String ENTITY_TYPES = REST.WebPath.Graph.GRAQL+"?query=match $a isa %s id '%s'; $b isa %s; ($a,$b); limit %s;&keyspace=%s&limitEmbedded=%s&infer=true&materialise=false";
    private static final String ROLE_TYPES = REST.WebPath.Graph.GRAQL+"?query=match $a isa %s id '%s'; ($a,%s:$b); limit %s;&keyspace=%s&limitEmbedded=%s&infer=true&materialise=false";

    private final EngineGraknGraphFactory factory;

    public DashboardController(EngineGraknGraphFactory factory, Service spark){
        this.factory = factory;

        spark.get(REST.WebPath.Dashboard.TYPES + ID_PARAMETER,         this::typesOfConcept);
        spark.get(REST.WebPath.Dashboard.EXPLORE + ID_PARAMETER,       this::exploreConcept);
        spark.get(REST.WebPath.Dashboard.EXPLAIN,       this::explainConcept);
        spark.get(REST.WebPath.Dashboard.PRECOMPUTE,    this::precomputeInferences);
    }

    @GET
    @Path("explore/{id}")
    @ApiOperation(
            value = "Return the HAL Explore representation for the given concept.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = IDENTIFIER,      value = "Identifier of the concept.", required = true, dataType = "string", paramType = "path"),
            @ApiImplicitParam(name = KEYSPACE,        value = "Name of graph to use.", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = OFFSET_EMBEDDED, value = "Offset to begin at for embedded HAL concepts.", required = true, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = LIMIT_EMBEDDED,  value = "Limit on the number of embedded HAL concepts.", required = true, dataType = "boolean", paramType = "query")
    })
    private Json exploreConcept(Request request, Response response){
        validateRequest(request);

        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        ConceptId conceptId = ConceptId.of(mandatoryRequestParameter(request, ID_PARAMETER));
        int offset = queryParameter(request, OFFSET_EMBEDDED).map(Integer::parseInt).orElse(0);
        int limit = queryParameter(request, LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);

        try(GraknGraph graph = factory.getGraph(keyspace, READ)){
            Concept concept = retrieveExistingConcept(graph, conceptId);
            Json body = Json.object();

            response.type(APPLICATION_HAL);
            response.status(200);
            body.set(RESPONSE,Json.read(HALExploreConcept(concept, keyspace, offset, limit)));
            response.body(body.toString());

            return body;
        }
    }

    @GET
    @Path("types/{id}")
    @ApiOperation(
            value = "Return a JSON object listing: " +
                    "- relationTypes the current concepts plays a role in." +
                    "- roleTypes played by all the other role players in all the relations the current concept takes part in" +
                    "- entityTypes that can play the roleTypes")
    @ApiImplicitParams({
            @ApiImplicitParam(name = IDENTIFIER,      value = "Identifier of the concept", required = true, dataType = "string", paramType = "path"),
            @ApiImplicitParam(name = KEYSPACE,        value = "Name of graph to use", required = true, dataType = "string", paramType = "query"),
    })
    private Json typesOfConcept(Request request, Response response){
        validateRequest(request);

        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        int limit = queryParameter(request, LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);
        ConceptId conceptId = ConceptId.of(mandatoryRequestParameter(request, ID_PARAMETER));

        try(GraknGraph graph = factory.getGraph(keyspace, READ)){
            Concept concept = retrieveExistingConcept(graph, conceptId);
            Json body = Json.object();
            Json responseField = Json.object();
            if (concept.isEntity()) {
                Collection<RoleType> rolesOfType = concept.asEntity().type().plays();

                responseField = Json.object(
                        "roles", getRoleTypes(rolesOfType, concept, limit, keyspace),
                        "relations", getRelationTypes(rolesOfType, concept, limit, keyspace),
                        "entities", getEntityTypes(rolesOfType, concept, limit, keyspace)
                );
            }
            response.status(200);
            body.set(RESPONSE,responseField);
            response.body(body.toString());

            return body;
        }
    }

    //TODO This should potentially be moved to the Graql controller
    @GET
    @Path("/explain")
    @ApiOperation(
            value = "Returns an HAL representation of the explanation tree for a given match query.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "query", value = "Match query to execute", required = true, dataType = "string", paramType = "query"),
    })
    private Json explainConcept(Request request, Response response) {
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        String queryString = mandatoryQueryParameter(request, QUERY);
        Json body = Json.object();

        try(GraknGraph graph = factory.getGraph(keyspace, READ)){
            Query<?> query = graph.graql().infer(true).parse(queryString);
            body.set(ORIGINAL_QUERY, query.toString());

            if(!(query instanceof MatchQuery)){
                throw new GraknEngineServerException(405, EXPLAIN_ONLY_MATCH, query.getClass().getName());
            }

            int limitEmbedded = queryParameter(request, REST.Request.Graql.LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);

            body.set(RESPONSE, explanationAnswersToHAL(((MatchQuery) query).admin().streamWithAnswers(), limitEmbedded));
        }
        response.status(200);
        response.body(body.toString());

        return body;
    }

    @GET
    @Path("/precomputeInferences")
    @ApiOperation(value = "Pre materialise results of all the rules on the graph.")
    @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query")
    private Boolean precomputeInferences(Request request, Response response) {
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);

        try(GraknGraph graph = factory.getGraph(keyspace, READ)){
            Reasoner.precomputeInferences(graph);

            response.status(200);

            return true;
        } catch (RuntimeException e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    private static List<Json> getRelationTypes(Collection<RoleType> roleTypesPlayerByConcept, Concept concept, int limit, String keyspace) {
        return roleTypesPlayerByConcept.stream().flatMap(roleType -> roleType.relationTypes().stream())
                .map(relationType -> relationType.getLabel().getValue()).sorted()
                .map(relationName -> Json.object("value", relationName, "href", String.format(RELATION_TYPES, concept.asInstance().type().getLabel().getValue(), concept.getId().getValue(), relationName, limit, keyspace, limit)))
                .collect(toList());
    }

    private static List<Json> getEntityTypes(Collection<RoleType> roleTypesPlayerByConcept, Concept concept, int limit, String keyspace) {
        return roleTypesPlayerByConcept.stream().flatMap(roleType -> roleType.relationTypes().stream())
                .flatMap(relationType -> relationType.relates().stream().filter(roleType1 -> !roleTypesPlayerByConcept.contains(roleType1)))
                .flatMap(roleType -> roleType.playedByTypes().stream().map(entityType -> entityType.getLabel().getValue()))
                .collect(Collectors.toSet()).stream()
                .sorted()
                .map(entityName -> Json.object("value", entityName, "href", String.format(ENTITY_TYPES, concept.asInstance().type().getLabel().getValue(), concept.getId().getValue(), entityName, limit, keyspace, limit)))
                .collect(toList());
    }

    private static List<Json> getRoleTypes(Collection<RoleType> roleTypesPlayerByConcept, Concept concept, int limit, String keyspace) {
        return roleTypesPlayerByConcept.stream().flatMap(roleType -> roleType.relationTypes().stream())
                .flatMap(relationType -> relationType.relates().stream().filter(roleType1 -> !roleTypesPlayerByConcept.contains(roleType1)))
                .map(roleType -> roleType.getLabel().getValue())
                .collect(Collectors.toSet()).stream()
                .sorted()
                .map(roleName -> Json.object("value", roleName, "href", String.format(ROLE_TYPES, concept.asInstance().type().getLabel().getValue(), concept.getId().getValue(), roleName, limit, keyspace, limit)))
                .collect(toList());
    }
}
