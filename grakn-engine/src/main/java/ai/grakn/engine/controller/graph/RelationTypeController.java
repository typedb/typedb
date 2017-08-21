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
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import com.codahale.metrics.MetricRegistry;
import mjson.Json;
import org.apache.commons.httpclient.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
public class RelationTypeController {
    private final EngineGraknGraphFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(EntityTypeController.class);

    public RelationTypeController(EngineGraknGraphFactory factory, Service spark,
                          MetricRegistry metricRegistry) {
        this.factory = factory;

        spark.get("/graph/relationType/:relationTypeLabel", this::getRelationType);
        spark.post("/graph/relationType", this::postRelationType);
        spark.post("/graph/relationType/:relationTypeLabel/relates/:roleLabel", null);
    }

    private Json getRelationType(Request request, Response response) {
        LOG.info("getRelationType - request received.");
        String relationTypeLabel = mandatoryPathParameter(request, "relationTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("getRelationType - attempting to find role " + relationTypeLabel + " in keyspace " + keyspace);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.READ)) {
            Optional<RelationType> relationType = Optional.ofNullable(graph.getRelationType(relationTypeLabel));
            if (relationType.isPresent()) {
                String jsonConceptId = relationType.get().getId().getValue();
                String jsonRelationTypeLabel = relationType.get().getLabel().getValue();
                response.status(HttpStatus.SC_OK);
                Json responseBody = Json.object("conceptId", jsonConceptId, "relationTypeLabel", jsonRelationTypeLabel);
                LOG.info("getRelationType - relationType found - " + jsonConceptId + ", " + jsonRelationTypeLabel + ". request processed.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.info("getRelationType - relationType NOT found. request processed.");
                return Json.nil();
            }
        }
    }

    private Json postRelationType(Request request, Response response) {
        LOG.info("postRelationType - request received.");
        Json requestBodyJson = Json.read(mandatoryBody(request));
        Map<String, Object> requestBody = requestBodyJson.asMap();
        String relationTypeLabel = (String) requestBody.get("relationTypeLabel");
        Stream<String> roleLabels = requestBodyJson.at("roleLabels").asList().stream().map(e -> (String) e);
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);

        LOG.info("postRelationType - attempting to add a new relationType " + relationTypeLabel + " on keyspace " + keyspace);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
            RelationType relationType = graph.putRelationType(relationTypeLabel);

            roleLabels.forEach(roleLabel -> {
                Role role = graph.putRole(roleLabel);
                relationType.relates(role);
            });

            graph.commit();
            String jsonConceptId = relationType.getId().getValue();
            String jsonRelationTypeLabel = relationType.getLabel().getValue();
            LOG.info("postRelationType - relationType " + jsonRelationTypeLabel + " with id " + jsonConceptId + " added. request processed.");
            response.status(HttpStatus.SC_OK);
            Json responseBody = Json.object("conceptId", jsonConceptId, "relationTypeLabel", jsonRelationTypeLabel);

            return responseBody;
        }
    }

    private Json assignRoleToRelationType(Request request, Response response) {
        return Json.nil();
    }
}
