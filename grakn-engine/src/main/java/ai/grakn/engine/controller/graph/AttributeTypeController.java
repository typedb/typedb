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
import ai.grakn.concept.ResourceType;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.exception.GraknServerException;
import com.codahale.metrics.MetricRegistry;
import mjson.Json;
import org.apache.commons.httpclient.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

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

public class AttributeTypeController {
    private final EngineGraknGraphFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(AttributeTypeController.class);

    public AttributeTypeController(EngineGraknGraphFactory factory, Service spark,
                                   MetricRegistry metricRegistry) {
        this.factory = factory;
        spark.post("/graph/resourceType", this::postResourceType);
        spark.get("/graph/resourceType/:resourceTypeLabel", this::getResourceType);
    }

    private Json postResourceType(Request request, Response response) {
        LOG.info("postResourceType - request received.");
        Map<String, Object> requestBody = Json.read(mandatoryBody(request)).asMap();
        String resourceTypeLabel = (String) requestBody.get("resourceTypeLabel");
        String resourceTypeDataTypeRaw = (String) requestBody.get("resourceTypeDataType");
        ResourceType.DataType<?> resourceTypeDataType = fromString(resourceTypeDataTypeRaw);
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postResourceType - attempting to add new resourceType " + resourceTypeLabel + " of type " + resourceTypeDataTypeRaw);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
            ResourceType resourceType = graph.putResourceType(resourceTypeLabel, resourceTypeDataType);
            graph.commit();
            String jsonConceptId = resourceType.getId().getValue();
            String jsonResourceTypeLabel = resourceType.getLabel().getValue();
            String jsonResourceTypeDataType = toString(resourceType.getDataType());
            LOG.info("postResourceType - resource type " + jsonResourceTypeLabel +
                " of type " + jsonResourceTypeDataType + " with id " + jsonConceptId + " added successfully. request processed.");
            response.status(HttpStatus.SC_OK);
            Json responseBody = Json.object(
                "conceptId", jsonConceptId,
                "resourceTypeLabel", jsonResourceTypeLabel,
                "resourceTypeDataType", jsonResourceTypeDataType
            );
            return responseBody;
        }
    }

    private Json getResourceType(Request request, Response response) {
        LOG.info("getResourceType - request received.");
        String resourceTypeLabel = mandatoryPathParameter(request, "resourceTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("getResourceType - attempting to find resourceType " + resourceTypeLabel + " in keyspace " + keyspace);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.READ)) {
            Optional<ResourceType> resourceType = Optional.ofNullable(graph.getResourceType(resourceTypeLabel));
            if (resourceType.isPresent()) {
                String jsonConceptId = resourceType.get().getId().getValue();
                String jsonResourceTypeLabel = resourceType.get().getLabel().getValue();
                String jsonResourceTypeDataType = toString(resourceType.get().getDataType());
                response.status(HttpStatus.SC_OK);
                Json responseBody = Json.object(
                    "conceptId", jsonConceptId,
                    "resourceTypeLabel", jsonResourceTypeLabel,
                    "resourceTypeDataType", jsonResourceTypeDataType
                );
                LOG.info("getResourceType - resourceType found - " + jsonConceptId + ", " +
                    jsonResourceTypeLabel + ", " + jsonResourceTypeDataType + ". request processed.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.info("getResourceType - resourceType NOT found. request processed.");
                return Json.nil();
            }
        }
    }

    private ResourceType.DataType<?> fromString(String dataType) {
        if (dataType.equals("string")) {
            return ResourceType.DataType.STRING;
        } else if (dataType.equals("double")) {
            return ResourceType.DataType.DOUBLE;
        } else if (dataType.equals("long")) {
            return ResourceType.DataType.LONG;
        } else if (dataType.equals("boolean")) {
            return ResourceType.DataType.BOOLEAN;
        } else {
            throw GraknServerException.invalidQueryExplaination("invalid datatype supplied: '" + dataType + "'");
        }
    }

    private String toString(ResourceType.DataType<?> dataType) {
        if (dataType.equals(ResourceType.DataType.STRING)) {
            return "string";
        } else if (dataType.equals(ResourceType.DataType.DOUBLE)) {
            return "double";
        } else if (dataType.equals(ResourceType.DataType.LONG)) {
            return "long";
        } else if (dataType.equals(ResourceType.DataType.BOOLEAN)) {
            return "boolean";
        } else {
            throw GraknServerException.invalidQueryExplaination("invalid datatype supplied: '" + dataType + "'");
        }
    }
}
