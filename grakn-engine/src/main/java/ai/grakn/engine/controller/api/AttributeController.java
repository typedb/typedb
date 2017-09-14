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
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.engine.factory.EngineGraknTxFactory;
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
import static ai.grakn.util.REST.Request.ATTRIBUTE_TYPE_LABEL_PARAMETER;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.WebPath.Api.ATTRIBUTE_TYPE;

/**
 * <p>
 *     Attribute endpoints
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class AttributeController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(AttributeController.class);

    public AttributeController(EngineGraknTxFactory factory, Service spark) {
        this.factory = factory;

        spark.post(ATTRIBUTE_TYPE + "/" + ATTRIBUTE_TYPE_LABEL_PARAMETER, this::postAttribute);
    }

    private Json postAttribute(Request request, Response response) {
        LOG.info("postAttribute - request received.");
        String attributeTypeLabel = mandatoryPathParameter(request, "attributeTypeLabel");
        Json requestBody = Json.read(mandatoryBody(request));
        String attributeValue = requestBody.at("value").asString();
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postAttribute - attempting to find attributeType " + attributeTypeLabel + " in keyspace " + keyspace);
        try (GraknTx graph = factory.tx(Keyspace.of(keyspace), GraknTxType.WRITE)) {
            Optional<AttributeType> attributeTypeOptional = Optional.ofNullable(graph.getAttributeType(attributeTypeLabel));
            if (attributeTypeOptional.isPresent()) {
                LOG.info("postAttribute - attributeType " + attributeTypeLabel + " found.");
                AttributeType attributeType = attributeTypeOptional.get();
                Attribute attribute = attributeType.putAttribute(attributeValue);
                graph.commit();

                String jsonConceptId = attribute.getId().getValue();
                Object jsonAttributeValue = attribute.getValue();
                LOG.info("postAttribute - attribute " + jsonConceptId + " of attributeType " + attributeTypeLabel + " added. request processed");
                response.status(HttpStatus.SC_OK);
                return attributeJson(jsonConceptId, jsonAttributeValue);
            } else {
                LOG.info("postAttribute - attributeType " + attributeTypeLabel + " NOT found.");
                response.status(HttpStatus.SC_BAD_REQUEST);
                return Json.nil();
            }
        }
    }

    private Json attributeJson(String conceptId, Object value) {
        return Json.object("attribute", Json.object(
                "conceptId", conceptId, "value", value
            )
        );
    }
}
