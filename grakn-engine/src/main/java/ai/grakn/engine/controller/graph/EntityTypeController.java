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
import ai.grakn.concept.EntityType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Role;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import com.codahale.metrics.MetricRegistry;
import mjson.Json;
import org.apache.commons.httpclient.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import java.util.Optional;

import static ai.grakn.engine.controller.util.Requests.mandatoryBody;
import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.engine.controller.util.Requests.mandatoryQueryParameter;
import static ai.grakn.util.REST.Request.KEYSPACE;

/**
 * <p>
 *     EntityType endpoints
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class EntityTypeController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(EntityTypeController.class);

    public EntityTypeController(EngineGraknTxFactory factory, Service spark,
                                MetricRegistry metricRegistry) {
        this.factory = factory;

        spark.get("/graph/entityType/:entityTypeLabel", this::getEntityType);
        spark.post("/graph/entityType", this::postEntityType);
        spark.delete("/graph/entityType/:entityTypeLabel", this::deleteEntityType); // TODO: implement
        spark.put("/graph/entityType/:entityTypeLabel/attribute/:attributeTypeLabel", this::assignAttributeTypeToEntityType);
        spark.delete("/graph/entityType/:entityTypeId/attribute/:attributeTypeId", this::deleteAttributeTypeToEntitiyTypeAssignment); // TODO: implement
        spark.put("/graph/entityType/:entityTypeId/plays/:roleTypeId", this::assignRoleToEntityType);
        spark.delete("/graph/entityType/:entityTypeId/plays/:roleTypeId", this::deleteRoleToEntitiyTypeAssignment); // TODO: implement
    }

    private Json getEntityType(Request request, Response response) {
        LOG.info("getEntityType - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, "entityTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("getEntityType - attempting to find entityType " + entityTypeLabel + " in keyspace " + keyspace);
        try (GraknTx graph = factory.tx(keyspace, GraknTxType.READ)) {
            Optional<EntityType> entityType = Optional.ofNullable(graph.getEntityType(entityTypeLabel));
            if (entityType.isPresent()) {
                String jsonConceptId = entityType.get().getId().getValue();
                String jsonEntityTypeLabel = entityType.get().getLabel().getValue();
                response.status(HttpStatus.SC_OK);
                Json responseBody = Json.object("conceptId", jsonConceptId, "entityTypeLabel", jsonEntityTypeLabel);
                LOG.info("getEntityType - entityType found - " + jsonConceptId + ", " + jsonEntityTypeLabel + ". request processed.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.info("getEntityType - entityType NOT found. request processed.");
                return Json.nil();
            }
        }
    }

    private Json postEntityType(Request request, Response response) {
        LOG.info("postEntityType - request received");
        Json requestBody = Json.read(mandatoryBody(request));
        String entityTypeLabel = (String) requestBody.asMap().get("entityTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postEntityType - attempting to add entityType " + entityTypeLabel + " in keyspace " + keyspace);
        try (GraknTx graph = factory.tx(keyspace, GraknTxType.WRITE)) {
            EntityType entityType = graph.putEntityType(entityTypeLabel);
            graph.commit();
            response.status(HttpStatus.SC_OK);
            String jsonConceptId = entityType.getId().getValue();
            String jsonEntityTypeLabel = entityType.getLabel().getValue();
            Json responseBody = Json.object("conceptId", jsonConceptId, "entityTypeLabel", jsonEntityTypeLabel);
            LOG.info("postEntityType - entityType added - " + jsonConceptId + ", " + jsonEntityTypeLabel + ". request processed.");
            return responseBody;
        }
    }

    private String deleteEntityType(Request request, Response response) {
        throw new UnsupportedOperationException("Unsupported operation: DELETE /entityType/:entityTypeId");
    }

    private Json assignAttributeTypeToEntityType(Request request, Response response) {
        LOG.info("assignAttributeTypeToEntityType - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, "entityTypeLabel");
        String attributeTypeLabel = mandatoryPathParameter(request, "attributeTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("assignAttributeTypeToEntityType - attempting to assign attributeType " + attributeTypeLabel + " to entityType " + entityTypeLabel + ", in keyspace " + keyspace);
        try (GraknTx graph = factory.tx(keyspace, GraknTxType.WRITE)) {
            Optional<EntityType> entityTypeOptional = Optional.ofNullable(graph.getEntityType(entityTypeLabel));
            Optional<AttributeType> attributeTypeOptional = Optional.ofNullable(graph.getAttributeType(attributeTypeLabel));
            if (entityTypeOptional.isPresent() && attributeTypeOptional.isPresent()) {

                EntityType entityType = entityTypeOptional.get();
                AttributeType attributeType = attributeTypeOptional.get();
                EntityType entityType1 = entityType.attribute(attributeType);
                graph.commit();
                Json responseBody = Json.object(
                    "conceptId", entityType1.getId().getValue(),
                    "entityTypeLabel", entityType1.getLabel().getValue()
                );
                LOG.info("assignAttributeTypeToEntityType - attributeType " + attributeTypeLabel  + " assigned to entityType " + entityTypeLabel + ". request processed.");
                response.status(HttpStatus.SC_OK);
                return responseBody;
            } else {
                LOG.info("assignAttributeTypeToEntityType - either entityType or attributeType not found. request processed.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

    private Json deleteAttributeTypeToEntitiyTypeAssignment(Request request, Response response) {
        throw new UnsupportedOperationException("Unsupported operation: DELETE /entityType/:entityTypeId/attribute/:attributeTypeId");
    }

    private Json assignRoleToEntityType(Request request, Response response) {
        LOG.info("assignAttributeTypeToEntityType - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, "entityTypeLabel");
        String roleLabel = mandatoryPathParameter(request, "roleLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("assignAttributeTypeToEntityType - attempting to assign roleLabel " + roleLabel + " to entityType " + entityTypeLabel + ", in keyspace " + keyspace);
        try (GraknTx graph = factory.tx(keyspace, GraknTxType.WRITE)) {
            Optional<EntityType> entityTypeOptional = Optional.ofNullable(graph.getEntityType(entityTypeLabel));
            Optional<Role> roleOptional = Optional.ofNullable(graph.getRole(roleLabel));

            if (entityTypeOptional.isPresent() && roleOptional.isPresent()) {
                EntityType entityType = entityTypeOptional.get();
                Role role = roleOptional.get();
                EntityType entityType1 = entityType.plays(role);
                graph.commit();
                Json responseBody = Json.object(
                    "conceptId", entityType1.getId().getValue(),
                    "entityTypeLabel", entityType1.getLabel().getValue(),
                    "roleLabel", role.getLabel()
                );
                response.status(HttpStatus.SC_OK);
                return responseBody;
            } else {
                LOG.info("assignAttributeTypeToEntityType - either entityType or role not found. request processed.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

    private Json deleteRoleToEntitiyTypeAssignment(Request request, Response response) {
        throw new UnsupportedOperationException("Unsupported operation: DELETE /entityType/:entityTypeId/attribute/:attributeTypeId");
    }
}
