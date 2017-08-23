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

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.engine.factory.EngineGraknTxFactory;
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
 *     Relationship endpoints
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class RelationshipController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(RelationshipController.class);

    public RelationshipController(EngineGraknTxFactory factory, Service spark) {
        this.factory = factory;

        spark.post("/graph/relationshipType/:relationshipTypeLabel", this::postRelationship);
        spark.put("/graph/relationship/:relationshipConceptId/role/:roleConceptId", this::assignEntityAndRoleToRelationship);
        // TODO: implement it after operation has been supported in the Graph API
//        spark.delete("/graph/relationship/:relationshipConceptId/role/:roleConceptId/entity/:entityConceptId", this::deleteEntityAndRoleToRelationshipAssignment);
    }

    private Json postRelationship(Request request, Response response) {
        LOG.info("postRelationship - request received.");
        String relationshipTypeLabel = mandatoryPathParameter(request, "relationshipTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postRelationship - attempting to find entityType " + relationshipTypeLabel + " in keyspace " + keyspace);
        try (GraknTx graph = factory.tx(keyspace, GraknTxType.WRITE)) {
            Optional<RelationshipType> relationshipTypeOptional = Optional.ofNullable(graph.getRelationshipType(relationshipTypeLabel));
            if (relationshipTypeOptional.isPresent()) {
                LOG.info("postRelationship - relationshipType " + relationshipTypeLabel + " found.");
                RelationshipType relationshipType = relationshipTypeOptional.get();
                Relationship relationship = relationshipType.addRelationship();
                String jsonConceptId = relationship.getId().getValue();
                LOG.info("postRelationship - relationship " + jsonConceptId + " of relationshipType " + relationshipTypeLabel + " added. request processed");
                response.status(HttpStatus.SC_OK);
                return relationshipJson(jsonConceptId);
            } else {
                LOG.info("postRelationship - relationshipType " + relationshipTypeLabel + " NOT found.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

    private Json assignEntityAndRoleToRelationship(Request request, Response response) {
        LOG.info("assignEntityAndRoleToRelationship - request received.");
        String relationshipConceptId = mandatoryPathParameter(request, "relationshipConceptId");
        String roleConceptId = mandatoryPathParameter(request, "roleConceptId");
        String entityConceptId = mandatoryPathParameter(request, "entityConceptId");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        try (GraknTx graph = factory.tx(keyspace, GraknTxType.WRITE)) {
            LOG.info("assignEntityAndRoleToRelationship - attempting to find roleConceptId " + roleConceptId + " and relationshipConceptId " + relationshipConceptId + ", in keyspace " + keyspace);
            Optional<Relationship> relationshipOptional = Optional.ofNullable(graph.getConcept(ConceptId.of(relationshipConceptId)));
            Optional<Role> roleOptional = Optional.ofNullable(graph.getConcept(ConceptId.of(roleConceptId)));
            Optional<Entity> entityOptional = Optional.ofNullable(graph.getConcept(ConceptId.of(entityConceptId)));

            if (relationshipOptional.isPresent() && roleOptional.isPresent() && entityOptional.isPresent()) {
                LOG.info("assignEntityAndRoleToRelationship - relationship, role and entity found. attempting to assign entity " + entityConceptId + " and role  " + roleConceptId + " to relationship " + relationshipConceptId);
                Relationship relationship = relationshipOptional.get();
                Role role = roleOptional.get();
                Entity entity = entityOptional.get();
                relationship.addRolePlayer(role, entity);
                graph.commit();
                LOG.info("assignEntityAndRoleToRelationship - assignment succeeded. request processed.");
                Json responseBody = Json.object();
                response.status(HttpStatus.SC_OK);
                return responseBody;
            } else {
                LOG.info("assignEntityAndRoleToRelationship - either entity, role or relationship not found. request processed.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

//    private Json deleteEntityAndRoleToRelationshipAssignment(Request request, Response response) {
//        throw new UnsupportedOperationException("Unsupported operation: DELETE /graph/entity/:conceptId/resource/:conceptId");
//    }

    private Json relationshipJson(String conceptId) {
        return Json.object("relationship", Json.object("conceptId", conceptId));
    }
}
