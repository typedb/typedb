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
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Attribute;
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

import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.engine.controller.util.Requests.mandatoryQueryParameter;
import static ai.grakn.util.REST.Request.KEYSPACE;

/**
 * <p>
 *     Entity endpoints
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class EntityController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(EntityTypeController.class);

    public EntityController(EngineGraknTxFactory factory, Service spark,
                                MetricRegistry metricRegistry) {
        this.factory = factory;

        spark.post("/graph/entityType/:entityTypeLabel/entity", this::postEntity);
        spark.put("/graph/entity/:entityConceptId/attribute/:attributeConceptId", this::assignAttributeToEntity);
        spark.delete("/graph/entity/:entityConceptId/attribute/:attributeConceptId", this::deleteAttributeToEntityAssignment); // TODO: implement
    }

    private Json postEntity(Request request, Response response) {
        LOG.info("postEntity - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, "entityTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postEntity - attempting to find entityType " + entityTypeLabel + " in keyspace " + keyspace);
        try (GraknTx graph = factory.tx(keyspace, GraknTxType.WRITE)) {
            Optional<EntityType> entityTypeOptional = Optional.ofNullable(graph.getEntityType(entityTypeLabel));
            if (entityTypeOptional.isPresent()) {
                LOG.info("postEntity - entityType " + entityTypeLabel + " found.");
                EntityType entityType = entityTypeOptional.get();
                Entity entity = entityType.addEntity();
                graph.commit();
                String jsonConceptId = entity.getId().getValue();
                LOG.info("postEntity - entity " + jsonConceptId + " of entityType " + entityTypeLabel + " added. request processed");
                response.status(HttpStatus.SC_OK);
                return Json.object("conceptId", jsonConceptId);
            } else {
                LOG.info("postEntity - entityType " + entityTypeLabel + " NOT found.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

    private Json assignAttributeToEntity(Request request, Response response) {
        LOG.info("assignAttributeToEntity - request received.");
        String entityConceptId = mandatoryPathParameter(request, "entityConceptId");
        String attributeConceptId = mandatoryPathParameter(request, "attributeConceptId");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        try (GraknTx graph = factory.tx(keyspace, GraknTxType.WRITE)) {
            LOG.info("assignAttributeToEntity - attempting to find attributeConceptId " + attributeConceptId + " and entityConceptId " + entityConceptId + ", in keyspace " + keyspace);
            Optional<Entity> entityOptional = Optional.ofNullable(graph.getConcept(ConceptId.of(entityConceptId)));
            Optional<Attribute> attributeOptional = Optional.ofNullable(graph.getConcept(ConceptId.of(attributeConceptId)));

            if (entityOptional.isPresent() && attributeOptional.isPresent()) {
                LOG.info("assignAttributeToEntity - entity and attribute found. attempting to assign attributeConceptId " + attributeConceptId + " to entityConceptId " + entityConceptId + ", in keyspace " + keyspace);
                Entity entity = entityOptional.get();
                Attribute attribute = attributeOptional.get();
                Entity entityType1 = entity.attribute(attribute);
                graph.commit();
                LOG.info("assignAttributeToEntity - assignment succeeded. request processed.");
                Json responseBody = Json.object();
                response.status(HttpStatus.SC_OK);
                return responseBody;
            } else {
                LOG.info("assignAttributeToEntity - either entity or attribute not found. request processed.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

    private Json deleteAttributeToEntityAssignment(Request request, Response response) {
        throw new UnsupportedOperationException("Unsupported operation: DELETE /graph/entity/:conceptId/attribute/:conceptId");
    }
}
