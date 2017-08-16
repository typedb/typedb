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
import ai.grakn.concept.EntityType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Role;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.exception.GraknServerException;
import com.codahale.metrics.MetricRegistry;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.Path;

import java.util.Map;
import java.util.Optional;

import static ai.grakn.engine.controller.util.Requests.mandatoryBody;
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

public class EntityTypeController {
    private final EngineGraknGraphFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(EntityTypeController.class);

    public EntityTypeController(EngineGraknGraphFactory factory, Service spark,
                                MetricRegistry metricRegistry) {
        this.factory = factory;

        spark.get("/graph/entityType/:entityTypeLabel", this::getEntityType);
        spark.post("/graph/entityType", this::postEntityType); // TODO:
        spark.delete("/graph/entityType/:entityTypeLabel", this::deleteEntityType);
        spark.post("/graph/entityType/:entityTypeLabel/resource/:resourceTypeLabel", this::assignResourceToEntityType);
        spark.post("/graph/entityType/:entityTypeId/resource/:resourceTypeId", this::deleteResourceToEntitiyTypeAssignment); // TODO
        spark.post("/graph/entityType/:entityTypeId/plays/:roleTypeId", this::assignRoleToEntityType);
    }

    private Json postEntityType(Request request, Response response) {
        LOG.info("postEntityType - request received");
        Json requestBody = Json.read(mandatoryBody(request));
        String entityTypeLabel = (String) requestBody.asMap().get("entityTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postEntityType - attempting to add entity " + entityTypeLabel + " in keyspace " + keyspace);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
            EntityType entityType = graph.putEntityType(entityTypeLabel);
            graph.commit();
            response.status(200);
            String jsonConceptId = entityType.getId().getValue();
            String jsonEntityTypeLabel = entityType.getLabel().getValue();
            Json responseBody = Json.object("conceptId", jsonConceptId, "entityTypeLabel", jsonEntityTypeLabel);
            LOG.info("postEntityType - entity added - " + jsonConceptId + ", " + jsonEntityTypeLabel + ". request processed.");
            return responseBody;
        }
    }

    private Json getEntityType(Request request, Response response) {
        LOG.info("getEntityType - request received.");
        String entityTypeLabel = mandatoryPathParameter(request, "entityTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("getEntityType - attempting to find entity " + entityTypeLabel + " in keyspace " + keyspace);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.READ)) {
            Optional<EntityType> entityType = Optional.ofNullable(graph.getEntityType(entityTypeLabel));
            if (entityType.isPresent()) {
                String jsonConceptId = entityType.get().getId().getValue();
                String jsonEntityTypeLabel = entityType.get().getLabel().getValue();
                response.status(200);
                Json responseBody = Json.object("conceptId", jsonConceptId, "entityTypeLabel", jsonEntityTypeLabel);
                LOG.info("getEntityType - entity found - " + jsonConceptId + ", " + jsonEntityTypeLabel + ". request processed.");
                return responseBody;
            } else {
                response.status(400);
                LOG.info("getEntityType - entity NOT found - request processed.");
                return Json.nil();
            }
        }
    }

    private String deleteEntityType(Request request, Response response) {
        throw new UnsupportedOperationException("Unsupported operation: DELETE /EntityType/:entityTypeId");
    }

    private Json assignResourceToEntityType(Request request, Response response) {
        String entityTypeLabel = mandatoryPathParameter(request, "entityTypeLabel");
        String resourceTypeLabel = mandatoryPathParameter(request, "resourceTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
            Optional<EntityType> entityTypeOptional = Optional.ofNullable(graph.getEntityType(entityTypeLabel));
            Optional<ResourceType> resourceTypeOptional = Optional.ofNullable(graph.getResourceType(resourceTypeLabel));
            if (entityTypeOptional.isPresent() && resourceTypeOptional.isPresent()) {
                EntityType entityType = entityTypeOptional.get();
                ResourceType resourceType = resourceTypeOptional.get();
                EntityType entityType1 = entityType.resource(resourceType);
                graph.commit();
                Json responseBody = Json.object(
                    "conceptId", entityType1.getId().getValue(),
                    "entityTypeLabel", entityType1.getLabel().getValue()
                );
                response.status(200);
                return responseBody;
            } else {
                response.status(400);
                return Json.nil();
            }
        }
    }

    private Json deleteResourceToEntitiyTypeAssignment(Request request, Response response) {
        throw new UnsupportedOperationException("Unsupported operation: DELETE /EntityType/:entityTypeId/resource/:resourceTypeId");
    }

    private Json assignRoleToEntityType(Request request, Response response) {
        String entityTypeLabel = mandatoryPathParameter(request, "entityTypeLabel");
        String roleLabel = mandatoryPathParameter(request, "roleLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);

        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
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
                response.status(200);
                return responseBody;
            } else {
                response.status(400);
                return Json.nil();
            }
        }
    }
}
