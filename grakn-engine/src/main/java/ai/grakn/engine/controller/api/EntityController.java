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
import ai.grakn.concept.Attribute;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
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
import static ai.grakn.util.REST.Request.ATTRIBUTE_CONCEPT_ID_PARAMETER;
import static ai.grakn.util.REST.Request.CONCEPT_ID_JSON_FIELD;
import static ai.grakn.util.REST.Request.ENTITY_CONCEPT_ID_PARAMETER;
import static ai.grakn.util.REST.Request.ENTITY_OBJECT_JSON_FIELD;
import static ai.grakn.util.REST.Request.ENTITY_TYPE_LABEL_PARAMETER;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.WebPath.Api.ENTITY_ATTRIBUTE_ASSIGNMENT;
import static ai.grakn.util.REST.WebPath.Api.ENTITY_TYPE;

/**
 * <p>
 *     A class which implements API endpoints for manipulating {@link Entity}
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class EntityController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(EntityController.class);

    public EntityController(EngineGraknTxFactory factory, Service spark) {
        this.factory = factory;

        spark.post(ENTITY_TYPE + "/" + ENTITY_TYPE_LABEL_PARAMETER, this::postEntity);
        spark.put(ENTITY_ATTRIBUTE_ASSIGNMENT, this::assignAttributeToEntity);
        spark.delete(ENTITY_ATTRIBUTE_ASSIGNMENT, this::deleteAttributeToEntityAssignment);
    }

    private Json postEntity(Request request, Response response) {
        LOG.debug("postEntity - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, ENTITY_TYPE_LABEL_PARAMETER);
        String keyspace = mandatoryPathParameter(request, KEYSPACE);
        LOG.debug("postEntity - attempting to find entityType " + entityTypeLabel + " in keyspace " + keyspace);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.WRITE)) {
            Optional<EntityType> entityTypeOptional = Optional.ofNullable(tx.getEntityType(entityTypeLabel));
            if (entityTypeOptional.isPresent()) {
                LOG.debug("postEntity - entityType " + entityTypeLabel + " found.");
                EntityType entityType = entityTypeOptional.get();
                Entity entity = entityType.addEntity();
                tx.commit();
                String jsonConceptId = entity.getId().getValue();
                LOG.debug("postEntity - entity " + jsonConceptId + " of entityType " + entityTypeLabel + " added. request processed");
                response.status(HttpStatus.SC_OK);
                return entityJson(jsonConceptId);
            } else {
                LOG.debug("postEntity - entityType " + entityTypeLabel + " NOT found.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

    private Json assignAttributeToEntity(Request request, Response response) {
        LOG.debug("assignAttributeToEntity - request received.");
        String entityConceptId = mandatoryPathParameter(request, ENTITY_CONCEPT_ID_PARAMETER);
        String attributeConceptId = mandatoryPathParameter(request, ATTRIBUTE_CONCEPT_ID_PARAMETER);
        String keyspace = mandatoryPathParameter(request, KEYSPACE);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.WRITE)) {
            LOG.debug("assignAttributeToEntity - attempting to find attributeConceptId " + attributeConceptId + " and entityConceptId " + entityConceptId + ", in keyspace " + keyspace);
            Optional<Entity> entityOptional = Optional.ofNullable(tx.getConcept(ConceptId.of(entityConceptId)));
            Optional<Attribute> attributeOptional = Optional.ofNullable(tx.getConcept(ConceptId.of(attributeConceptId)));

            if (entityOptional.isPresent() && attributeOptional.isPresent()) {
                LOG.debug("assignAttributeToEntity - entity and attribute found. attempting to assign attributeConceptId " + attributeConceptId + " to entityConceptId " + entityConceptId + ", in keyspace " + keyspace);
                Entity entity = entityOptional.get();
                Attribute attribute = attributeOptional.get();
                entity.attribute(attribute);
                tx.commit();
                LOG.debug("assignAttributeToEntity - assignment succeeded. request processed.");
                Json responseBody = Json.object();
                response.status(HttpStatus.SC_OK);
                return responseBody;
            } else {
                String entityInfo = entityOptional.map(e -> e.toString()).orElse("<empty>");
                String attributeInfo = attributeOptional.map(e -> e.toString()).orElse("<empty>");
                LOG.debug("assignAttributeToEntity - either entity (" + entityInfo + ") or" +
                    "attribute (" + attributeInfo + ") not found. request processed.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

    private Json deleteAttributeToEntityAssignment(Request request, Response response) {
        LOG.debug("deleteAttributeToEntityAssignment - request received.");
        String entityConceptId = mandatoryPathParameter(request, ENTITY_CONCEPT_ID_PARAMETER);
        String attributeConceptId = mandatoryPathParameter(request, ATTRIBUTE_CONCEPT_ID_PARAMETER);
        String keyspace = mandatoryPathParameter(request, KEYSPACE);

        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.WRITE)) {
            LOG.debug("deleteAttributeToEntityAssignment - attempting to find attributeConceptId " + attributeConceptId + " and entityConceptId " + entityConceptId + ", in keyspace " + keyspace);
            Optional<Entity> entityOptional = Optional.ofNullable(tx.getConcept(ConceptId.of(entityConceptId)));
            Optional<Attribute> attributeOptional = Optional.ofNullable(tx.getConcept(ConceptId.of(attributeConceptId)));

            if (entityOptional.isPresent() && attributeOptional.isPresent()) {
                LOG.debug("deleteAttributeToEntityAssignment - entity and attribute found. attempting to assign attributeConceptId " + attributeConceptId + " to entityConceptId " + entityConceptId + ", in keyspace " + keyspace);
                Entity entity = entityOptional.get();
                Attribute attribute = attributeOptional.get();
                entity.deleteAttribute(attribute);
                tx.commit();
                LOG.debug("deleteAttributeToEntityAssignment - deletion succeeded. request processed.");
                Json responseBody = Json.object();
                response.status(HttpStatus.SC_OK);
                return responseBody;
            } else {
                String entityInfo = entityOptional.map(e -> e.toString()).orElse("<empty>");
                String attributeInfo = attributeOptional.map(e -> e.toString()).orElse("<empty>");
                LOG.debug("deleteAttributeToEntityAssignment - either entity (" + entityInfo + ") or" +
                    "attribute (" + attributeInfo + ") not found. request processed.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }

    }

    private Json entityJson(String conceptId) {
        return Json.object(ENTITY_OBJECT_JSON_FIELD, Json.object(CONCEPT_ID_JSON_FIELD, conceptId));
    }
}
