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
import ai.grakn.concept.RuleType;
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
import static ai.grakn.util.REST.Request.KEYSPACE;

/**
 * <p>
 *     RuleType endpoints
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class RuleTypeController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(RuleTypeController.class);

    public RuleTypeController(EngineGraknTxFactory factory, Service spark) {
        this.factory = factory;
        spark.get("/api/ruleType/:ruleTypeLabel", this::getRuleType);
        spark.post("/api/ruleType", this::postRuleType);
    }

    private Json getRuleType(Request request, Response response) {
        LOG.info("getRuleType - request received.");
        String ruleTypeLabel = mandatoryPathParameter(request, "ruleTypeLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("getRuleType - attempting to find rule " + ruleTypeLabel + " in keyspace " + keyspace);
        try (GraknTx tx = factory.tx(keyspace, GraknTxType.READ)) {
            Optional<RuleType> ruleType = Optional.ofNullable(tx.getRuleType(ruleTypeLabel));
            if (ruleType.isPresent()) {
                String jsonConceptId = ruleType.get().getId().getValue();
                String jsonRuleTypeLabel = ruleType.get().getLabel().getValue();
                response.status(HttpStatus.SC_OK);
                Json responseBody = ruleTypeJson(jsonConceptId, jsonRuleTypeLabel);
                LOG.info("getRuleType - ruleType found - " + jsonConceptId + ", " + jsonRuleTypeLabel + ". request processed.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.info("getRuleType - ruleType NOT found. request processed.");
                return Json.nil();
            }
        }
    }

    private Json postRuleType(Request request, Response response) {
        LOG.info("postRuleType - request received.");
        Json requestBody = Json.read(mandatoryBody(request));
        String ruleTypeLabel = requestBody.at("ruleType").at("label").asString();
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postRuleType - attempting to add a new ruleType " + ruleTypeLabel + " on keyspace " + keyspace);
        try (GraknTx tx = factory.tx(keyspace, GraknTxType.WRITE)) {
            RuleType ruleType = tx.putRuleType(ruleTypeLabel);
            tx.commit();
            String jsonConceptId = ruleType.getId().getValue();
            String jsonRuleTypeLabel = ruleType.getLabel().getValue();
            LOG.info("postRuleType - ruleType " + jsonRuleTypeLabel + " with id " + jsonConceptId + " added. request processed.");
            response.status(HttpStatus.SC_OK);
            Json responseBody = ruleTypeJson(jsonConceptId, ruleTypeLabel);

            return responseBody;
        }
    }

    private Json ruleTypeJson(String conceptId, String label) {
        return Json.object(
            "ruleType", Json.object(
                "conceptId", conceptId, "label", label
            )
        );
    }
}
