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

import ai.grakn.GraknTx;
import ai.grakn.Keyspace;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Role;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknServerException;
import ai.grakn.graql.GetQuery;
import ai.grakn.graql.Printer;
import ai.grakn.graql.Query;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.printer.Printers;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.util.REST;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static ai.grakn.GraknTxType.READ;
import static ai.grakn.GraknTxType.WRITE;
import static ai.grakn.engine.controller.ConceptController.mandatoryRequestParameter;
import static ai.grakn.engine.controller.ConceptController.retrieveExistingConcept;
import static ai.grakn.engine.controller.ConceptController.validateRequest;
import static ai.grakn.engine.controller.util.Requests.mandatoryQueryParameter;
import static ai.grakn.engine.controller.util.Requests.queryParameter;
import static ai.grakn.graql.internal.hal.HALBuilder.HALExploreConcept;
import static ai.grakn.graql.internal.hal.HALBuilder.explanationAnswersToHAL;
import static ai.grakn.util.REST.Request.Concept.LIMIT_EMBEDDED;
import static ai.grakn.util.REST.Request.Concept.OFFSET_EMBEDDED;
import static ai.grakn.util.REST.Request.Graql.QUERY;
import static ai.grakn.util.REST.Request.ID_PARAMETER;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_HAL;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static ai.grakn.util.REST.Response.Graql.IDENTIFIER;
import static ai.grakn.util.REST.Response.Graql.ORIGINAL_QUERY;
import static java.util.stream.Collectors.toList;

/**
 * <p>
 * Private endpoints used by dashboard to query by concept type.
 * </p>
 * <p>
 * <p>
 * This class should be thought of as a workplace/staging point for potential future user-facing endpoints.
 * </p>
 *
 * @author alexandraorth
 */
@Path("/dashboard")
public class DashboardController {

    private static final String RELATION_TYPES = REST.WebPath.KB.GRAQL + "?query=match $a isa %s id '%s'; ($a,$b) isa %s; limit %s;&keyspace=%s&limitEmbedded=%s&infer=true";
    private static final String ENTITY_TYPES = REST.WebPath.KB.GRAQL + "?query=match $a isa %s id '%s'; $b isa %s; ($a,$b); limit %s;&keyspace=%s&limitEmbedded=%s&infer=true";
    private static final String ROLE_TYPES = REST.WebPath.KB.GRAQL + "?query=match $a isa %s id '%s'; ($a,%s:$b); limit %s;&keyspace=%s&limitEmbedded=%s&infer=true";

    private final EngineGraknTxFactory factory;

    public DashboardController(EngineGraknTxFactory factory, Service spark) {
        this.factory = factory;

        spark.get(REST.WebPath.Dashboard.TYPES + ID_PARAMETER, this::typesOfConcept);
        spark.get(REST.WebPath.Dashboard.EXPLORE + ID_PARAMETER, this::exploreConcept);
        spark.get(REST.WebPath.Dashboard.EXPLAIN, this::explainConcept);
    }

    @GET
    @Path("explore/{id}")
    @ApiOperation(
            value = "Return the HAL Explore representation for the given concept.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = IDENTIFIER, value = "Identifier of the concept.", required = true, dataType = "string", paramType = "path"),
            @ApiImplicitParam(name = KEYSPACE, value = "Name of graph to use.", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = OFFSET_EMBEDDED, value = "Offset to begin at for embedded HAL concepts.", required = true, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = LIMIT_EMBEDDED, value = "Limit on the number of embedded HAL concepts.", required = true, dataType = "boolean", paramType = "query")
    })
    private Json exploreConcept(Request request, Response response) {
        validateRequest(request, APPLICATION_HAL);

        Keyspace keyspace = Keyspace.of(mandatoryQueryParameter(request, KEYSPACE));
        ConceptId conceptId = ConceptId.of(mandatoryRequestParameter(request, ID_PARAMETER));
        int offset = queryParameter(request, OFFSET_EMBEDDED).map(Integer::parseInt).orElse(0);
        int limit = queryParameter(request, LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);

        try (GraknTx graph = factory.tx(keyspace, READ)) {
            Concept concept = retrieveExistingConcept(graph, conceptId);

            response.type(APPLICATION_HAL);
            response.status(200);

            return Json.read(HALExploreConcept(concept, keyspace, offset, limit));
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
            @ApiImplicitParam(name = IDENTIFIER, value = "Identifier of the concept", required = true, dataType = "string", paramType = "path"),
            @ApiImplicitParam(name = KEYSPACE, value = "Name of graph to use", required = true, dataType = "string", paramType = "query"),
    })
    private Json typesOfConcept(Request request, Response response) {
        validateRequest(request, APPLICATION_JSON);

        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        int limit = queryParameter(request, LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);
        ConceptId conceptId = ConceptId.of(mandatoryRequestParameter(request, ID_PARAMETER));

        try (GraknTx graph = factory.tx(keyspace, READ)) {
            Concept concept = retrieveExistingConcept(graph, conceptId);
            Json body = Json.object();
            Json responseField = Json.object();
            if (concept.isEntity()) {
                Collection<Role> rolesOfType = concept.asEntity().type().plays().collect(Collectors.toSet());

                responseField = Json.object(
                        "roles", getRoleTypes(rolesOfType, concept, limit, keyspace),
                        "relations", getRelationTypes(rolesOfType, concept, limit, keyspace),
                        "entities", getEntityTypes(rolesOfType, concept, limit, keyspace)
                );
            }
            response.status(200);
            response.body(responseField.toString());

            return body;
        }
    }

    //TODO This should potentially be moved to the Graql controller
    @GET
    @Path("/explain")
    @ApiOperation(
            value = "Returns an HAL representation of the explanation tree for a given get query.")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "keyspace", value = "Name of graph to use", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "query", value = "Get query to execute", required = true, dataType = "string", paramType = "query"),
    })
    private Json explainConcept(Request request, Response response) {
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        String queryString = mandatoryQueryParameter(request, QUERY);
        Json body = Json.object();

        try (GraknTx graph = factory.tx(keyspace, WRITE)) {
            Query<?> query = graph.graql().infer(true).parse(queryString);
            body.set(ORIGINAL_QUERY, query.toString());

            if (!(query instanceof GetQuery)) {
                throw GraknServerException.invalidQueryExplaination(query.getClass().getName());
            }

            int limitEmbedded = queryParameter(request, REST.Request.Graql.LIMIT_EMBEDDED).map(Integer::parseInt).orElse(-1);
            response.status(200);
            Printer<?> printer = Printers.hal(graph.keyspace(), limitEmbedded);
            Answer answer = ((GetQuery) query).execute().stream().findFirst().orElse(new QueryAnswer());
            return explanationAnswersToHAL(answer.getExplanation().getAnswers(), printer);

        }

    }

    private static List<Json> getRelationTypes(Collection<Role> roleTypesPlayerByConcept, Concept concept, int limit, String keyspace) {
        return roleTypesPlayerByConcept.stream().flatMap(roleType -> roleType.relationshipTypes())
                .map(relationType -> relationType.getLabel().getValue()).sorted()
                .map(relationName -> Json.object("value", relationName, "href", String.format(RELATION_TYPES, concept.asThing().type().getLabel().getValue(), concept.getId().getValue(), relationName, limit, keyspace, limit)))
                .collect(toList());
    }

    private static List<Json> getEntityTypes(Collection<Role> roleTypesPlayerByConcept, Concept concept, int limit, String keyspace) {
        return roleTypesPlayerByConcept.stream().flatMap(roleType -> roleType.relationshipTypes())
                .flatMap(relationType -> relationType.relates().filter(roleType1 -> !roleTypesPlayerByConcept.contains(roleType1)))
                .flatMap(roleType -> roleType.playedByTypes().map(entityType -> entityType.getLabel().getValue()))
                .collect(Collectors.toSet()).stream()
                .sorted()
                .map(entityName -> Json.object("value", entityName, "href", String.format(ENTITY_TYPES, concept.asThing().type().getLabel().getValue(), concept.getId().getValue(), entityName, limit, keyspace, limit)))
                .collect(toList());
    }

    private static List<Json> getRoleTypes(Collection<Role> roleTypesPlayerByConcept, Concept concept, int limit, String keyspace) {
        return roleTypesPlayerByConcept.stream().flatMap(roleType -> roleType.relationshipTypes())
                .flatMap(relationType -> relationType.relates().filter(roleType1 -> !roleTypesPlayerByConcept.contains(roleType1)))
                .map(roleType -> roleType.getLabel().getValue())
                .collect(Collectors.toSet()).stream()
                .sorted()
                .map(roleName -> Json.object("value", roleName, "href", String.format(ROLE_TYPES, concept.asThing().type().getLabel().getValue(), concept.getId().getValue(), roleName, limit, keyspace, limit)))
                .collect(toList());
    }
}
