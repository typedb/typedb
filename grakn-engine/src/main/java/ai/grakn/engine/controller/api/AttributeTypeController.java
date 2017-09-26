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
import ai.grakn.graql.internal.parser.QueryParserImpl;
import mjson.Json;
import org.apache.commons.httpclient.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import java.util.Optional;

import static ai.grakn.engine.controller.util.Requests.extractJsonField;
import static ai.grakn.engine.controller.util.Requests.mandatoryBody;
import static ai.grakn.engine.controller.util.Requests.mandatoryPathParameter;
import static ai.grakn.engine.controller.util.Requests.mandatoryQueryParameter;
import static ai.grakn.util.REST.Request.ATTRIBUTE_TYPE_LABEL_PARAMETER;
import static ai.grakn.util.REST.Request.ATTRIBUTE_TYPE_OBJECT_JSON_FIELD;
import static ai.grakn.util.REST.Request.CONCEPT_ID_JSON_FIELD;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Request.LABEL_JSON_FIELD;
import static ai.grakn.util.REST.Request.TYPE_JSON_FIELD;
import static ai.grakn.util.REST.WebPath.Api.ATTRIBUTE_TYPE;

/**
 * <p>
 *     A class which implements API endpoints for manipulating {@link AttributeType}
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class AttributeTypeController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(AttributeTypeController.class);

    public AttributeTypeController(EngineGraknTxFactory factory, Service spark) {
        this.factory = factory;
        spark.post(ATTRIBUTE_TYPE, this::postAttributeType);
        spark.get(ATTRIBUTE_TYPE + "/" + ATTRIBUTE_TYPE_LABEL_PARAMETER, this::getAttributeType);
    }

    private Json postAttributeType(Request request, Response response) {
        LOG.debug("postAttributeType - request received.");
        Json requestBody = Json.read(mandatoryBody(request));
        String attributeTypeLabel = extractJsonField(requestBody, ATTRIBUTE_TYPE_OBJECT_JSON_FIELD, LABEL_JSON_FIELD).asString();
        String attributeTypeDataTypeRaw = extractJsonField(requestBody, ATTRIBUTE_TYPE_OBJECT_JSON_FIELD, TYPE_JSON_FIELD).asString();
        AttributeType.DataType<?> attributeTypeDataType = fromString(attributeTypeDataTypeRaw);
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.debug("postAttributeType - attempting to add new attributeType " + attributeTypeLabel + " of type " + attributeTypeDataTypeRaw);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.WRITE)) {
            AttributeType attributeType = tx.putAttributeType(attributeTypeLabel, attributeTypeDataType);
            tx.commit();
            String jsonConceptId = attributeType.getId().getValue();
            String jsonAttributeTypeLabel = attributeType.getLabel().getValue();
            String jsonAttributeTypeDataType = toString(attributeType.getDataType());
            LOG.debug("postAttributeType - attribute type " + jsonAttributeTypeLabel +
                " of type " + jsonAttributeTypeDataType + " with id " + jsonConceptId + " added successfully. request processed.");
            response.status(HttpStatus.SC_OK);
            Json responseBody = attributeTypeJson(jsonConceptId, jsonAttributeTypeLabel, jsonAttributeTypeDataType);
            return responseBody;
        }
    }

    private Json getAttributeType(Request request, Response response) {
        LOG.debug("getAttributeType - request received.");
        String attributeTypeLabel = mandatoryPathParameter(request, ATTRIBUTE_TYPE_LABEL_PARAMETER);
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.debug("getAttributeType - attempting to find attributeType " + attributeTypeLabel + " in keyspace " + keyspace);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.READ)) {
            Optional<AttributeType> attributeType = Optional.ofNullable(tx.getAttributeType(attributeTypeLabel));
            if (attributeType.isPresent()) {
                String jsonConceptId = attributeType.get().getId().getValue();
                String jsonAttributeTypeLabel = attributeType.get().getLabel().getValue();
                String jsonAttributeTypeDataType = toString(attributeType.get().getDataType());
                response.status(HttpStatus.SC_OK);
                Json responseBody = attributeTypeJson(jsonConceptId, jsonAttributeTypeLabel, jsonAttributeTypeDataType);
                LOG.debug("getAttributeType - attributeType found - " + jsonConceptId + ", " +
                    jsonAttributeTypeLabel + ", " + jsonAttributeTypeDataType + ". request processed.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.debug("getAttributeType - attributeType NOT found. request processed.");
                return Json.nil();
            }
        }
    }

    private AttributeType.DataType<?> fromString(String dataType) {
        Optional<AttributeType.DataType> fromStringOpt =
            Optional.ofNullable(QueryParserImpl.DATA_TYPES.get(dataType));

        return fromStringOpt.orElseThrow(() ->
            GraknServerException.invalidQueryExplaination("invalid data type supplied: '" + dataType + "'")
        );
    }

    private String toString(AttributeType.DataType<?> dataType) {
        Optional<String> toStringOpt =
            Optional.ofNullable(QueryParserImpl.DATA_TYPES.inverse().get(dataType));

        return toStringOpt.orElseThrow(() ->
            GraknServerException.invalidQueryExplaination("invalid data type supplied: '" + dataType + "'")
        );
    }

    private Json attributeTypeJson(String conceptId, String label, String dataType) {
        return Json.object(ATTRIBUTE_TYPE_OBJECT_JSON_FIELD, Json.object(
                CONCEPT_ID_JSON_FIELD, conceptId,
                LABEL_JSON_FIELD, label,
                TYPE_JSON_FIELD, dataType
            )
        );
    }
}
