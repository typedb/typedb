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
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
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

import static ai.grakn.engine.controller.util.Requests.mandatoryBody;
import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.engine.controller.util.Requests.mandatoryQueryParameter;
import static ai.grakn.util.REST.Request.KEYSPACE;

/**
 * <p>
 *     Endpoint tests for Graph API
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class ResourceController {
    private final EngineGraknGraphFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(ResourceTypeController.class);

    public ResourceController(EngineGraknGraphFactory factory, Service spark,
                              MetricRegistry metricRegistry) {
        this.factory = factory;
        spark.post("/graph/resourceType/:resourceTypeLabel/resource", this::postResource);
    }

    private Json postResource(Request request, Response response) {
        LOG.info("postResource - request received.");
        String resourceTypeLabel = mandatoryPathParameter(request, "resourceTypeLabel");
        Json requestBody = Json.read(mandatoryBody(request));
        String resourceValue = (String) requestBody.asMap().get("resourceValue");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postResource - attempting to find resourceType " + resourceTypeLabel + " in keyspace " + keyspace);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
            Optional<ResourceType> resourceTypeOptional = Optional.ofNullable(graph.getResourceType(resourceTypeLabel));
            if (resourceTypeOptional.isPresent()) {
                LOG.info("postResource - resourceType " + resourceTypeLabel + " found.");
                ResourceType resourceType = resourceTypeOptional.get();
                Resource resource = resourceType.putResource(resourceValue);
                String jsonConceptId = resource.getId().getValue();
                Object jsonResourceValue = resource.getValue();
                LOG.info("postResource - resource " + jsonConceptId + " of resourceType " + resourceTypeLabel + " added. request processed");
                response.status(HttpStatus.SC_OK);
                return Json.object("conceptId", jsonConceptId, "resourceValue", jsonResourceValue);
            } else {
                LOG.info("postResource - resourceType " + resourceTypeLabel + " NOT found.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }
}
