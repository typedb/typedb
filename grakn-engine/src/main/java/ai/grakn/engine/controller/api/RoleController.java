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

import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.util.REST.Request.CONCEPT_ID_JSON_FIELD;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Request.LABEL_JSON_FIELD;
import static ai.grakn.util.REST.Request.ROLE_LABEL_PARAMETER;
import static ai.grakn.util.REST.Request.ROLE_OBJECT_JSON_FIELD;
import static ai.grakn.util.REST.WebPath.Api.ROLE;

/**
 * <p>
 *     A class which implements API endpoints for manipulating {@link Role}
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class RoleController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(RoleController.class);

    public RoleController(EngineGraknTxFactory factory, Service spark) {
        this.factory = factory;

        spark.get(ROLE + "/" + ROLE_LABEL_PARAMETER, this::getRole);
    }

    private Json getRole(Request request, Response response) {
        LOG.debug("getRole - request received.");
        String roleLabel = mandatoryPathParameter(request, ROLE_LABEL_PARAMETER);
        String keyspace = mandatoryPathParameter(request, KEYSPACE);
        LOG.debug("getRole - attempting to find role " + roleLabel + " in keyspace " + keyspace);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.READ)) {
            Optional<Role> role = Optional.ofNullable(tx.getRole(roleLabel));
            if (role.isPresent()) {
                String jsonConceptId = role.get().getId().getValue();
                String jsonRoleLabel = role.get().getLabel().getValue();
                response.status(HttpStatus.SC_OK);
                Json responseBody = roleJson(jsonConceptId, jsonRoleLabel);
                LOG.debug("getRole - role found - " + jsonConceptId + ", " + jsonRoleLabel + ". request processed.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.debug("getRole - role NOT found. request processed.");
                return Json.nil();
            }
        }
    }

    private Json roleJson(String conceptId, String roleLabel) {
        return Json.object(ROLE_OBJECT_JSON_FIELD, Json.object(
            CONCEPT_ID_JSON_FIELD, conceptId, LABEL_JSON_FIELD, roleLabel)
        );
    }
}