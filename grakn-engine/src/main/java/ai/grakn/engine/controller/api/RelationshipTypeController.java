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
import ai.grakn.concept.RelationshipType;
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
import java.util.stream.Stream;

import static ai.grakn.engine.controller.util.Requests.extractJsonField;
import static ai.grakn.engine.controller.util.Requests.mandatoryBody;
import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.util.REST.Request.CONCEPT_ID_JSON_FIELD;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Request.LABEL_JSON_FIELD;
import static ai.grakn.util.REST.Request.RELATIONSHIP_TYPE_LABEL_PARAMETER;
import static ai.grakn.util.REST.Request.RELATIONSHIP_TYPE_OBJECT_JSON_FIELD;
import static ai.grakn.util.REST.Request.ROLE_ARRAY_JSON_FIELD;
import static ai.grakn.util.REST.WebPath.Api.RELATIONSHIP_TYPE;

/**
 * <p>
 *     A class which implements API endpoints for manipulating {@link RelationshipType}
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */
public class RelationshipTypeController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(RelationshipTypeController.class);

    public RelationshipTypeController(EngineGraknTxFactory factory, Service spark) {
        this.factory = factory;

        spark.get(RELATIONSHIP_TYPE + "/" + RELATIONSHIP_TYPE_LABEL_PARAMETER, this::getRelationshipType);
        spark.post(RELATIONSHIP_TYPE, this::postRelationshipType);
//        spark.post("/api/relationshipType/:relationshipTypeLabel/relates/:roleLabel", null); // TODO 
    }

    private Json getRelationshipType(Request request, Response response) {
        LOG.debug("getRelationshipType - request received.");
        String relationshipTypeLabel = mandatoryPathParameter(request, RELATIONSHIP_TYPE_LABEL_PARAMETER);
        String keyspace = mandatoryPathParameter(request, KEYSPACE);
        LOG.debug("getRelationshipType - attempting to find role " + relationshipTypeLabel + " in keyspace " + keyspace);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.READ)) {
            Optional<RelationshipType> relationshipType = Optional.ofNullable(tx.getRelationshipType(relationshipTypeLabel));
            if (relationshipType.isPresent()) {
                String jsonConceptId = relationshipType.get().getId().getValue();
                String jsonRelationshipTypeLabel = relationshipType.get().getLabel().getValue();
                response.status(HttpStatus.SC_OK);
                Json responseBody = relationshipTypeJson(jsonConceptId, jsonRelationshipTypeLabel);
                LOG.debug("getRelationshipType - relationshipType found - " + jsonConceptId + ", " + jsonRelationshipTypeLabel + ". request processed.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.debug("getRelationshipType - relationshipType NOT found. request processed.");
                return Json.nil();
            }
        }
    }

    private Json postRelationshipType(Request request, Response response) {
        LOG.debug("postRelationshipType - request received.");
        Json requestBody = Json.read(mandatoryBody(request));
        String relationshipTypeLabel = extractJsonField(requestBody, RELATIONSHIP_TYPE_OBJECT_JSON_FIELD, LABEL_JSON_FIELD).asString();
        Stream<String> roleLabels = extractJsonField(requestBody, RELATIONSHIP_TYPE_OBJECT_JSON_FIELD, ROLE_ARRAY_JSON_FIELD).asList().stream().map(e -> (String) e);
        String keyspace = mandatoryPathParameter(request, KEYSPACE);

        LOG.debug("postRelationshipType - attempting to add a new relationshipType " + relationshipTypeLabel + " on keyspace " + keyspace);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.WRITE)) {
            RelationshipType relationshipType = tx.putRelationshipType(relationshipTypeLabel);

            roleLabels.forEach(roleLabel -> {
                Role role = tx.putRole(roleLabel);
                relationshipType.relates(role);
            });

            tx.commit();
            String jsonConceptId = relationshipType.getId().getValue();
            String jsonRelationshipTypeLabel = relationshipType.getLabel().getValue();
            LOG.debug("postRelationshipType - relationshipType " + jsonRelationshipTypeLabel + " with id " + jsonConceptId + " added. request processed.");
            response.status(HttpStatus.SC_OK);
            Json responseBody = relationshipTypeJson(jsonConceptId, jsonRelationshipTypeLabel);

            return responseBody;
        }
    }

    private Json relationshipTypeJson(String conceptId, String label) {
        return Json.object(RELATIONSHIP_TYPE_OBJECT_JSON_FIELD, Json.object(
            CONCEPT_ID_JSON_FIELD, conceptId, LABEL_JSON_FIELD, label
            )
        );
    }
}
