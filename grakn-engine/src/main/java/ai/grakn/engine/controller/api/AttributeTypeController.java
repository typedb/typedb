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
import ai.grakn.concept.AttributeType;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknServerException;
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
 *     AttributeType endpoints
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class AttributeTypeController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(AttributeTypeController.class);

    public AttributeTypeController(EngineGraknTxFactory factory, Service spark) {
        this.factory = factory;
        spark.post("/api/attributeType", this::postAttributeType);
        spark.get("/api/attributeType/:attributeTypeLabel", this::getAttributeType);
    }

    private Json postAttributeType(Request request, Response response) {
        LOG.info("postAttributeType - request received.");
        Json requestBody = Json.read(mandatoryBody(request));
        String attributeTypeLabel = requestBody.at("attributeType").at("label").asString();
        String attributeTypeDataTypeRaw = requestBody.at("attributeType").at("type").asString();
        AttributeType.DataType<?> attributeTypeDataType = fromString(attributeTypeDataTypeRaw);
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postAttributeType - attempting to add new attributeType " + attributeTypeLabel + " of type " + attributeTypeDataTypeRaw);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.WRITE)) {
            AttributeType attributeType = tx.putAttributeType(attributeTypeLabel, attributeTypeDataType);
            tx.commit();
            String jsonConceptId = attributeType.getId().getValue();
            String jsonAttributeTypeLabel = attributeType.getLabel().getValue();
            String jsonAttributeTypeDataType = toString(attributeType.getDataType());
            LOG.info("postAttributeType - attribute type " + jsonAttributeTypeLabel +
                " of type " + jsonAttributeTypeDataType + " with id " + jsonConceptId + " added successfully. request processed.");
            response.status(HttpStatus.SC_OK);
            Json responseBody = attributeTypeJson(jsonConceptId, jsonAttributeTypeLabel, jsonAttributeTypeDataType);
            return responseBody;
        }
    }

    private Json getAttributeType(Request request, Response response) {
        LOG.info("getAttributeType - request received.");
        String attributeTypeLabel = mandatoryPathParameter(request, "attributeTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("getAttributeType - attempting to find attributeType " + attributeTypeLabel + " in keyspace " + keyspace);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.READ)) {
            Optional<AttributeType> attributeType = Optional.ofNullable(tx.getAttributeType(attributeTypeLabel));
            if (attributeType.isPresent()) {
                String jsonConceptId = attributeType.get().getId().getValue();
                String jsonAttributeTypeLabel = attributeType.get().getLabel().getValue();
                String jsonAttributeTypeDataType = toString(attributeType.get().getDataType());
                response.status(HttpStatus.SC_OK);
                Json responseBody = attributeTypeJson(jsonConceptId, jsonAttributeTypeLabel, jsonAttributeTypeDataType);
                LOG.info("getAttributeType - attributeType found - " + jsonConceptId + ", " +
                    jsonAttributeTypeLabel + ", " + jsonAttributeTypeDataType + ". request processed.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.info("getAttributeType - attributeType NOT found. request processed.");
                return Json.nil();
            }
        }
    }

    private AttributeType.DataType<?> fromString(String dataType) {
        if (dataType.equals("string")) {
            return AttributeType.DataType.STRING;
        } else if (dataType.equals("double")) {
            return AttributeType.DataType.DOUBLE;
        } else if (dataType.equals("long")) {
            return AttributeType.DataType.LONG;
        } else if (dataType.equals("boolean")) {
            return AttributeType.DataType.BOOLEAN;
        } else {
            throw GraknServerException.invalidQueryExplaination("invalid datatype supplied: '" + dataType + "'");
        }
    }

    private String toString(AttributeType.DataType<?> dataType) {
        if (dataType.equals(AttributeType.DataType.STRING)) {
            return "string";
        } else if (dataType.equals(AttributeType.DataType.DOUBLE)) {
            return "double";
        } else if (dataType.equals(AttributeType.DataType.LONG)) {
            return "long";
        } else if (dataType.equals(AttributeType.DataType.BOOLEAN)) {
            return "boolean";
        } else {
            throw GraknServerException.invalidQueryExplaination("invalid datatype supplied: '" + dataType + "'");
        }
    }

    private Json attributeTypeJson(String conceptId, String label, String dataType) {
        return Json.object("attributeType", Json.object(
                "conceptId", conceptId,
                "label", label,
                "type", dataType
            )
        );
    }
}
