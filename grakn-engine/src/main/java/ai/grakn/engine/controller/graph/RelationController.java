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

package ai.grakn.engine.controller.graph;

import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import com.codahale.metrics.MetricRegistry;
import mjson.Json;
import org.apache.commons.httpclient.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import java.util.Optional;

import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.engine.controller.util.Requests.mandatoryQueryParameter;
import static ai.grakn.util.REST.Request.KEYSPACE;

/**
 * <p>
 *     Endpoints for Graph API
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class RelationController {
    private final EngineGraknGraphFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(EntityTypeController.class);

    public RelationController(EngineGraknGraphFactory factory, Service spark,
                            MetricRegistry metricRegistry) {
        this.factory = factory;

        spark.post("/graph/relationType/:relationTypeLabel/relation", this::postRelation);
        spark.put("/graph/relation/:relationConceptId/role/:roleConceptId", this::assignEntityAndRoleToRelation);
        spark.delete("/graph/relation/:relationConceptId/role/:roleConceptId/entity/:entityConceptId", this::deleteEntityAndRoleToRelationAssignment);
    }

    private Json postRelation(Request request, Response response) {
        LOG.info("postRelation - request received.");
        String relationTypeLabel = mandatoryPathParameter(request, "relationTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postRelation - attempting to find entityType " + relationTypeLabel + " in keyspace " + keyspace);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
            Optional<RelationType> relationTypeOptional = Optional.ofNullable(graph.getRelationType(relationTypeLabel));
            if (relationTypeOptional.isPresent()) {
                LOG.info("postRelation - relationType " + relationTypeLabel + " found.");
                RelationType relationType = relationTypeOptional.get();
                Relation relation = relationType.addRelation();
                String jsonConceptId = relation.getId().getValue();
                LOG.info("postRelation - relation " + jsonConceptId + " of relationType " + relationTypeLabel + " added. request processed");
                response.status(HttpStatus.SC_OK);
                return Json.object("conceptId", jsonConceptId);
            } else {
                LOG.info("postRelation - relationType " + relationTypeLabel + " NOT found.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

    private Json assignEntityAndRoleToRelation(Request request, Response response) {
        LOG.info("assignEntityAndRoleToRelation - request received.");
        String relationConceptId = mandatoryPathParameter(request, "relationConceptId");
        String roleConceptId = mandatoryPathParameter(request, "roleConceptId");
        String entityConceptId = mandatoryPathParameter(request, "entityConceptId");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
            LOG.info("assignEntityAndRoleToRelation - attempting to find roleConceptId " + roleConceptId + " and relationConceptId " + relationConceptId + ", in keyspace " + keyspace);
            Optional<Relation> relationOptional = Optional.ofNullable(graph.getConcept(ConceptId.of(relationConceptId)));
            Optional<Role> roleOptional = Optional.ofNullable(graph.getConcept(ConceptId.of(roleConceptId)));
            Optional<Entity> entityOptional = Optional.ofNullable(graph.getConcept(ConceptId.of(entityConceptId)));

            if (relationOptional.isPresent() && roleOptional.isPresent() && entityOptional.isPresent()) {
                LOG.info("assignEntityAndRoleToRelation - relation, role and entity found. attempting to assign entity " + entityConceptId + " and role  " + roleConceptId + " to relation " + relationConceptId);
                Relation relation = relationOptional.get();
                Role role = roleOptional.get();
                Entity entity = entityOptional.get();
                Relation entityType1 = relation.addRolePlayer(role, entity);
                graph.commit();
                LOG.info("assignEntityAndRoleToRelation - assignment succeeded. request processed.");
                Json responseBody = Json.object();
                response.status(HttpStatus.SC_OK);
                return responseBody;
            } else {
                LOG.info("assignEntityAndRoleToRelation - either entity, role or relation not found. request processed.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

    private Json deleteEntityAndRoleToRelationAssignment(Request request, Response response) {
        throw new UnsupportedOperationException("Unsupported operation: DELETE /graph/entity/:conceptId/resource/:conceptId");
    }
}
