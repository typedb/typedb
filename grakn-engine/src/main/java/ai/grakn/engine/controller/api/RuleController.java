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
import ai.grakn.concept.Rule;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.graql.Pattern;
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
 *     Rule endpoints
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class RuleController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(RuleController.class);

    public RuleController(EngineGraknTxFactory factory, Service spark) {
        this.factory = factory;
        spark.get("/api/rule/:ruleLabel", this::getRule);
        spark.post("/api/rule", this::postRule);
    }

    private Json getRule(Request request, Response response) {
        LOG.info("getRule - request received.");
        String ruleLabel = mandatoryPathParameter(request, "ruleLabel");
        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("getRule - attempting to find rule " + ruleLabel + " in keyspace " + keyspace);
        try (GraknTx tx = factory.tx(keyspace, GraknTxType.READ)) {
            Optional<Rule> rule = Optional.ofNullable(tx.getRule(ruleLabel));
            if (rule.isPresent()) {
                String jsonConceptId = rule.get().getId().getValue();
                String jsonRuleLabel = rule.get().getLabel().getValue();
                response.status(HttpStatus.SC_OK);
                Json responseBody = ruleJson(jsonConceptId, jsonRuleLabel);
                LOG.info("getRule - rule found - " + jsonConceptId + ", " + jsonRuleLabel + ". request processed.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.info("getRule - rule NOT found. request processed.");
                return Json.nil();
            }
        }
    }

    private Json postRule(Request request, Response response) {
        LOG.info("postRule - request received.");
        Json requestBody = Json.read(mandatoryBody(request));
        String ruleLabel = requestBody.at("rule").at("label").asString();
        String when = requestBody.at("rule").at("when").asString();
        String then = requestBody.at("rule").at("then").asString();

        String keyspace = mandatoryQueryParameter(request, KEYSPACE);
        LOG.info("postRule - attempting to add a new rule " + ruleLabel + " on keyspace " + keyspace);
        try (GraknTx tx = factory.tx(keyspace, GraknTxType.WRITE)) {
            Rule rule = tx.putRule(
                ruleLabel,
                tx.graql().parsePattern(when),
                tx.graql().parsePattern(then)
            );
            tx.commit();
            String jsonConceptId = rule.getId().getValue();
            String jsonRuleLabel = rule.getLabel().getValue();
            LOG.info("postRule - rule " + jsonRuleLabel + " with id " + jsonConceptId + " added. request processed.");
            response.status(HttpStatus.SC_OK);
            Json responseBody = ruleJson(jsonConceptId, ruleLabel);

            return responseBody;
        }
    }

    private Json ruleJson(String conceptId, String label) {
        return Json.object(
            "rule", Json.object(
                "conceptId", conceptId, "label", label
            )
        );
    }
}
