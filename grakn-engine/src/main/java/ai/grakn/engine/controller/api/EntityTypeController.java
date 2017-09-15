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

package ai.grakn.engine.controller.api;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.AttributeType;
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

import static ai.grakn.engine.controller.util.Requests.mandatoryBody;
import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.engine.controller.util.Requests.mandatoryQueryParameter;
import static ai.grakn.util.REST.Request.ATTRIBUTE_TYPE_LABEL_PARAMETER;
import static ai.grakn.util.REST.Request.CONCEPT_ID_JSON_FIELD;
import static ai.grakn.util.REST.Request.ENTITY_TYPE_LABEL_PARAMETER;
import static ai.grakn.util.REST.Request.ENTITY_TYPE_OBJECT_JSON_FIELD;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Request.LABEL_JSON_FIELD;
import static ai.grakn.util.REST.WebPath.Api.ENTITY_TYPE;
import static ai.grakn.util.REST.WebPath.Api.ENTITY_TYPE_ATTRIBUTE_TYPE_ASSIGNMENT;

/**
 * <p>
 *     A class which implements API endpoints for manipulating {@link EntityType}
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class EntityTypeController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(EntityTypeController.class);

    public EntityTypeController(EngineGraknTxFactory factory, Service spark) {
        this.factory = factory;

        spark.get(ENTITY_TYPE + "/" + ENTITY_TYPE_LABEL_PARAMETER, this::getEntityType);
        spark.post(ENTITY_TYPE, this::postEntityType);
        // TODO: implement it after operation has been supported in the Graph API
//        spark.delete("/api/entityType/:entityTypeLabel", this::deleteEntityType);
        spark.put(ENTITY_TYPE_ATTRIBUTE_TYPE_ASSIGNMENT, this::assignAttributeTypeToEntityType);
        // TODO: implement it after operation has been supported in the Graph API
//        spark.delete("/api/entityType/:entityTypeId/attribute/:attributeTypeId", this::deleteAttributeTypeToEntitiyTypeAssignment);
        // TODO: implement it after operation has been supported in the Graph API
//        spark.delete("/api/entityType/:entityTypeId/plays/:roleTypeId", this::deleteRoleToEntitiyTypeAssignment);
    }

    private Json getEntityType(Request request, Response response) {
        LOG.debug("getEntityType - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, ENTITY_TYPE_LABEL_PARAMETER);
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.debug("getEntityType - attempting to find entityType " + entityTypeLabel + " in keyspace " + keyspace);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.READ)) {
            Optional<EntityType> entityType = Optional.ofNullable(tx.getEntityType(entityTypeLabel));
            if (entityType.isPresent()) {
                String jsonConceptId = entityType.get().getId().getValue();
                String jsonEntityTypeLabel = entityType.get().getLabel().getValue();
                response.status(HttpStatus.SC_OK);
                Json responseBody = entityTypeJson(jsonConceptId, jsonEntityTypeLabel);
                LOG.debug("getEntityType - entityType found - " + jsonConceptId + ", " + jsonEntityTypeLabel + ". request processed.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.debug("getEntityType - entityType NOT found. request processed.");
                return Json.nil();
            }
        }
    }

    private Json postEntityType(Request request, Response response) {
        LOG.debug("postEntityType - request received");
        Json requestBody = Json.read(mandatoryBody(request));
        String entityTypeLabel = requestBody.at(ENTITY_TYPE_OBJECT_JSON_FIELD).at(LABEL_JSON_FIELD).asString();
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.debug("postEntityType - attempting to add entityType " + entityTypeLabel + " in keyspace " + keyspace);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.WRITE)) {
            EntityType entityType = tx.putEntityType(entityTypeLabel);
            tx.commit();
            response.status(HttpStatus.SC_OK);
            String jsonConceptId = entityType.getId().getValue();
            String jsonEntityTypeLabel = entityType.getLabel().getValue();
            Json responseBody = entityTypeJson(jsonConceptId, jsonEntityTypeLabel);
            LOG.debug("postEntityType - entityType added - " + jsonConceptId + ", " + jsonEntityTypeLabel + ". request processed.");
            return responseBody;
        }
    }

//    private String deleteEntityType(Request request, Response response) {
//        throw new UnsupportedOperationException("Unsupported operation: DELETE /entityType/:entityTypeId");
//    }

    private Json assignAttributeTypeToEntityType(Request request, Response response) {
        LOG.debug("assignAttributeTypeToEntityType - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, ENTITY_TYPE_LABEL_PARAMETER);
        String attributeTypeLabel = mandatoryPathParameter(request, ATTRIBUTE_TYPE_LABEL_PARAMETER);
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.debug("assignAttributeTypeToEntityType - attempting to assign attributeType " + attributeTypeLabel + " to entityType " + entityTypeLabel + ", in keyspace " + keyspace);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.WRITE)) {
            Optional<EntityType> entityTypeOptional = Optional.ofNullable(tx.getEntityType(entityTypeLabel));
            Optional<AttributeType> attributeTypeOptional = Optional.ofNullable(tx.getAttributeType(attributeTypeLabel));
            if (entityTypeOptional.isPresent() && attributeTypeOptional.isPresent()) {

                EntityType entityType = entityTypeOptional.get();
                AttributeType attributeType = attributeTypeOptional.get();
                entityType.attribute(attributeType);
                tx.commit();
                LOG.debug("assignAttributeTypeToEntityType - attributeType " + attributeTypeLabel  + " assigned to entityType " + entityTypeLabel + ". request processed.");
                response.status(HttpStatus.SC_OK);
                return Json.nil();
            } else {
                LOG.debug("assignAttributeTypeToEntityType - either entityType or attributeType not found. request processed.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

//    private Json deleteAttributeTypeToEntitiyTypeAssignment(Request request, Response response) {
//        throw new UnsupportedOperationException("Unsupported operation: DELETE /entityType/:entityTypeId/attribute/:attributeTypeId");
//    }


//    private Json deleteRoleToEntitiyTypeAssignment(Request request, Response response) {
//        throw new UnsupportedOperationException("Unsupported operation: DELETE /entityType/:entityTypeId/attribute/:attributeTypeId");
//    }


    private Json entityTypeJson(String conceptId, String label) {
        return Json.object(ENTITY_TYPE_OBJECT_JSON_FIELD, Json.object(CONCEPT_ID_JSON_FIELD, conceptId, LABEL_JSON_FIELD, label));
    }
}
