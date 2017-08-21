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
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Resource;
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

public class EntityController {
    private final EngineGraknGraphFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(EntityTypeController.class);

    public EntityController(EngineGraknGraphFactory factory, Service spark,
                                MetricRegistry metricRegistry) {
        this.factory = factory;

        spark.post("/graph/entityType/:entityTypeLabel/entity", this::postEntity);
        spark.put("/graph/entity/:entityConceptId/resource/:resourceConceptId", this::assignResourceToEntity);
        spark.delete("/graph/entity/:entityConceptId/resource/:resourceConceptId", this::deleteResourceToEntityAssignment);
    }

    private Json postEntity(Request request, Response response) {
        LOG.info("postEntity - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, "entityTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postEntity - attempting to find entityType " + entityTypeLabel + " in keyspace " + keyspace);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
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

    private Json assignResourceToEntity(Request request, Response response) {
        LOG.info("assignResourceToEntity - request received.");
        String entityConceptId = mandatoryPathParameter(request, "entityConceptId");
        String resourceConceptId = mandatoryPathParameter(request, "resourceConceptId");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
            LOG.info("assignResourceToEntity - attempting to find resourceConceptId " + resourceConceptId + " and entityConceptId " + entityConceptId + ", in keyspace " + keyspace);
            Optional<Entity> entityOptional = Optional.ofNullable(graph.getConcept(ConceptId.of(entityConceptId)));
            Optional<Resource> resourceOptional = Optional.ofNullable(graph.getConcept(ConceptId.of(resourceConceptId)));

            if (entityOptional.isPresent() && resourceOptional.isPresent()) {
                LOG.info("assignResourceToEntity - entity and resource found. attempting to assign resourceConceptId " + resourceConceptId + " to entityConceptId " + entityConceptId + ", in keyspace " + keyspace);
                Entity entity = entityOptional.get();
                Resource resource = resourceOptional.get();
                Entity entityType1 = entity.resource(resource);
                graph.commit();
                LOG.info("assignResourceToEntity - assignment succeeded. request processed.");
                Json responseBody = Json.object();
                response.status(HttpStatus.SC_OK);
                return responseBody;
            } else {
                LOG.info("assignResourceToEntity - either entity or resource not found. request processed.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

    private Json deleteResourceToEntityAssignment(Request request, Response response) {
        throw new UnsupportedOperationException("Unsupported operation: DELETE /graph/entity/:conceptId/resource/:conceptId");
    }
}
