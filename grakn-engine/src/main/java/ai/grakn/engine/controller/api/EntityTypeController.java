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
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import mjson.Json;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import java.util.Optional;
import java.util.function.BiConsumer;

import static ai.grakn.engine.controller.util.Requests.extractJsonField;
import static ai.grakn.engine.controller.util.Requests.mandatoryBody;
import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
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
        spark.delete(ENTITY_TYPE + "/" + ENTITY_TYPE_LABEL_PARAMETER, this::deleteEntityType);

        spark.put(ENTITY_TYPE_ATTRIBUTE_TYPE_ASSIGNMENT, this::assignAttributeTypeToEntityType);
        spark.delete(ENTITY_TYPE_ATTRIBUTE_TYPE_ASSIGNMENT, this::deleteAttributeTypeToEntityTypeAssignment);
    }

    private Json getEntityType(Request request, Response response) {
        LOG.debug("getEntityType - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, ENTITY_TYPE_LABEL_PARAMETER);
        String keyspace = mandatoryPathParameter(request, KEYSPACE);
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
        String entityTypeLabel = extractJsonField(requestBody, ENTITY_TYPE_OBJECT_JSON_FIELD, LABEL_JSON_FIELD).asString();
        String keyspace = mandatoryPathParameter(request, KEYSPACE);
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

    public Json deleteEntityType(Request request, Response response) {
        LOG.debug("deleteEntityType - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, ENTITY_TYPE_LABEL_PARAMETER);
        String keyspace = mandatoryPathParameter(request, KEYSPACE);
        LOG.debug("deleteEntityType - attempting to find entityType " + entityTypeLabel + " in keyspace " + keyspace);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.READ)) {
            Optional<EntityType> entityTypeOptional = Optional.ofNullable(tx.getEntityType(entityTypeLabel));
            if (entityTypeOptional.isPresent()) {
                EntityType entityType = entityTypeOptional.get();
                entityType.delete();
                response.status(HttpStatus.SC_OK);
                Json responseBody = Json.object();
                LOG.debug("deleteEntityType - entityType " + entityTypeLabel + " deleted.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.debug("deleteEntityType - entityType " + entityTypeLabel + " NOT found. request processed.");
                return Json.nil();
            }
        }
    }

    private Json assignAttributeTypeToEntityType(Request request, Response response) {
        LOG.debug("assignAttributeTypeToEntityType - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, ENTITY_TYPE_LABEL_PARAMETER);
        String attributeTypeLabel = mandatoryPathParameter(request, ATTRIBUTE_TYPE_LABEL_PARAMETER);
        String keyspace = mandatoryPathParameter(request, KEYSPACE);
        LOG.debug("assignAttributeTypeToEntityType - attempting to assign attributeType " + attributeTypeLabel + " to entityType " + entityTypeLabel + ", in keyspace " + keyspace);
        BiConsumer<EntityType, AttributeType> whenFound = (entity, attribute) -> {
            entity.attribute(attribute);
            LOG.debug("assignAttributeTypeToEntityType - attributeType " + attributeTypeLabel  + " assigned to entityType " + entityTypeLabel + ". request processed.");
        };

        BiConsumer<Optional<EntityType>, Optional<AttributeType>> whenNotFound =
            (entityTypeOptional, attributeTypeOptional) -> {
                String entityInfo = entityTypeOptional.map(e -> e.toString()).orElse("<empty>");
                String attributeInfo = attributeTypeOptional.map(e -> e.toString()).orElse("<empty>");
                LOG.debug("assignAttributeTypeToEntityType - either entityType '" + entityInfo +
                    "' or attributeType '" + attributeInfo + "' not found. request processed.");
            };

        Pair<Integer, Json> assignAttribute = withEntityAndAttribute(keyspace, entityTypeLabel,
            attributeTypeLabel, whenFound, whenNotFound);

        response.status(assignAttribute.getLeft());
        return assignAttribute.getRight();
    }

    private Json deleteAttributeTypeToEntityTypeAssignment(Request request, Response response) {
        LOG.debug("deleteAttributeTypeToEntityTypeAssignment - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, ENTITY_TYPE_LABEL_PARAMETER);
        String attributeTypeLabel = mandatoryPathParameter(request, ATTRIBUTE_TYPE_LABEL_PARAMETER);
        String keyspace = mandatoryPathParameter(request, KEYSPACE);
        LOG.debug("deleteAttributeTypeToEntityTypeAssignment - attempting to assign attributeType " + attributeTypeLabel + " to entityType " + entityTypeLabel + ", in keyspace " + keyspace);

        BiConsumer<EntityType, AttributeType> whenFound = (entity, attribute) -> {
            entity.deleteAttribute(attribute);
            LOG.debug("deleteAttributeTypeToEntityTypeAssignment - attributeType " + attributeTypeLabel  + " assigned to entityType " + entityTypeLabel + ". request processed.");
        };

        BiConsumer<Optional<EntityType>, Optional<AttributeType>> whenNotFound =
            (entityTypeOptional, attributeTypeOptional) -> {
                String entityInfo = entityTypeOptional.map(e -> e.toString()).orElse("<empty>");
                String attributeInfo = attributeTypeOptional.map(e -> e.toString()).orElse("<empty>");
                LOG.debug("deleteAttributeTypeToEntityTypeAssignment - either entityType '" + entityInfo +
                    "' or attributeType '" + attributeInfo + "' not found. request processed.");
        };

        Pair<Integer, Json> deleteAttributeAssignment = withEntityAndAttribute(keyspace, entityTypeLabel,
            attributeTypeLabel, whenFound, whenNotFound);

        response.status(deleteAttributeAssignment.getLeft());
        return deleteAttributeAssignment.getRight();
    }

    private Json entityTypeJson(String conceptId, String label) {
        return Json.object(ENTITY_TYPE_OBJECT_JSON_FIELD, Json.object(CONCEPT_ID_JSON_FIELD, conceptId, LABEL_JSON_FIELD, label));
    }

    private Pair<Integer, Json> withEntityAndAttribute(String keyspace, String entityTypeLabel, String attributeTypeLabel,
                                                       BiConsumer<EntityType, AttributeType> whenFound,
                                                       BiConsumer<Optional<EntityType>, Optional<AttributeType>> whenNotFound) {
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.WRITE)) {
            Optional<EntityType> entityTypeOptional = Optional.ofNullable(tx.getEntityType(entityTypeLabel));
            Optional<AttributeType> attributeTypeOptional = Optional.ofNullable(tx.getAttributeType(attributeTypeLabel));
            if (entityTypeOptional.isPresent() && attributeTypeOptional.isPresent()) {

                EntityType entityType = entityTypeOptional.get();
                AttributeType attributeType = attributeTypeOptional.get();
                whenFound.accept(entityType, attributeType);
                tx.commit();

                return Pair.of(HttpStatus.SC_OK, Json.object());
            } else {
                whenNotFound.accept(entityTypeOptional, attributeTypeOptional);
                return Pair.of(HttpStatus.SC_BAD_REQUEST, Json.nil());
            }
        }
    }
}
