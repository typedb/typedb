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
public class RoleController {
    private final EngineGraknGraphFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(EntityTypeController.class);

    public RoleController(EngineGraknGraphFactory factory, Service spark,
                          MetricRegistry metricRegistry) {
        this.factory = factory;

        spark.get("/graph/role/:roleLabel", this::getRole);
        spark.post("/graph/role", this::postRole);
    }

    private Json postRole(Request request, Response response) {
        LOG.info("postRole - request received.");
        Map<String, Object> requestBody = Json.read(mandatoryBody(request)).asMap();
        String roleLabel = (String) requestBody.get("roleLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postRole - attempting to add a new role " + roleLabel + " on keyspace " + keyspace);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.WRITE)) {
            Role role = graph.putRole(roleLabel);
            graph.commit();
            String jsonConceptId = role.getId().getValue();
            String jsonRoleLabel = role.getLabel().getValue();
            LOG.info("postRole - role " + jsonRoleLabel + " with id " + jsonConceptId + " added. request processed.");
            response.status(HttpStatus.SC_OK);
            Json responseBody = Json.object("conceptId", jsonConceptId, "roleLabel", jsonRoleLabel);

            return responseBody;
        }
    }

    private Json getRole(Request request, Response response) {
        LOG.info("getRole - request received.");
        String roleLabel = mandatoryPathParameter(request, "roleLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("getRole - attempting to find role " + roleLabel + " in keyspace " + keyspace);
        try (GraknGraph graph = factory.getGraph(keyspace, GraknTxType.READ)) {
            Optional<Role> role = Optional.ofNullable(graph.getRole(roleLabel));
            if (role.isPresent()) {
                String jsonConceptId = role.get().getId().getValue();
                String jsonRoleLabel = role.get().getLabel().getValue();
                response.status(HttpStatus.SC_OK);
                Json responseBody = Json.object("conceptId", jsonConceptId, "roleLabel", jsonRoleLabel);
                LOG.info("getRole - role found - " + jsonConceptId + ", " + jsonRoleLabel + ". request processed.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.info("getRole - role NOT found. request processed.");
                return Json.nil();
            }
        }
    }
}