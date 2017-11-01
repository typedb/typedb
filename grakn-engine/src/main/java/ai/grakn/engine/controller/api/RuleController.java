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
import ai.grakn.concept.Rule;
import ai.grakn.engine.factory.EngineGraknTxFactory;
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
import static ai.grakn.util.REST.Request.CONCEPT_ID_JSON_FIELD;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Request.LABEL_JSON_FIELD;
import static ai.grakn.util.REST.Request.RULE_LABEL_PARAMETER;
import static ai.grakn.util.REST.Request.RULE_OBJECT_JSON_FIELD;
import static ai.grakn.util.REST.Request.THEN_JSON_FIELD;
import static ai.grakn.util.REST.Request.WHEN_JSON_FIELD;
import static ai.grakn.util.REST.WebPath.Api.RULE;

/**
 * <p>
 *     A class which implements API endpoints for manipulating {@link Rule}
 * </p>
 *
 * @author Ganeshwara Herawan Hananda
 */

public class RuleController {
    private final EngineGraknTxFactory factory;
    private static final Logger LOG = LoggerFactory.getLogger(RuleController.class);

    public RuleController(EngineGraknTxFactory factory, Service spark) {
        this.factory = factory;
        spark.get(RULE + "/" + RULE_LABEL_PARAMETER, this::getRule);
        spark.post(RULE, this::postRule);
    }

    private Json getRule(Request request, Response response) {
        LOG.debug("getRule - request received.");
        String ruleLabel = mandatoryPathParameter(request, RULE_LABEL_PARAMETER);
        String keyspace = mandatoryPathParameter(request, KEYSPACE);
        LOG.debug("getRule - attempting to find rule " + ruleLabel + " in keyspace " + keyspace);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.READ)) {
            Optional<Rule> rule = Optional.ofNullable(tx.getRule(ruleLabel));
            if (rule.isPresent()) {
                String jsonConceptId = rule.get().getId().getValue();
                String jsonRuleLabel = rule.get().getLabel().getValue();
                String jsonRuleWhen = rule.get().getWhen().toString();
                String jsonRuleThen = rule.get().getThen().toString();
                response.status(HttpStatus.SC_OK);
                Json responseBody = ruleJson(jsonConceptId, jsonRuleLabel, jsonRuleWhen, jsonRuleThen);
                LOG.debug("getRule - rule found - " + jsonConceptId + ", " + jsonRuleLabel + ". request processed.");
                return responseBody;
            } else {
                response.status(HttpStatus.SC_BAD_REQUEST);
                LOG.debug("getRule - rule NOT found. request processed.");
                return Json.nil();
            }
        }
    }

    private Json postRule(Request request, Response response) {
        LOG.debug("postRule - request received.");
        Json requestBody = Json.read(mandatoryBody(request));
        String ruleLabel = extractJsonField(requestBody, RULE_OBJECT_JSON_FIELD, LABEL_JSON_FIELD).asString();
        String when = extractJsonField(requestBody, RULE_OBJECT_JSON_FIELD, WHEN_JSON_FIELD).asString();
        String then = extractJsonField(requestBody, RULE_OBJECT_JSON_FIELD, THEN_JSON_FIELD).asString();

        String keyspace = mandatoryPathParameter(request, KEYSPACE);
        LOG.debug("postRule - attempting to add a new rule " + ruleLabel + " on keyspace " + keyspace);
        try (GraknTx tx = factory.tx(Keyspace.of(keyspace), GraknTxType.WRITE)) {
            Rule rule = tx.putRule(
                ruleLabel,
                    tx.graql().parser().parsePattern(when),
                    tx.graql().parser().parsePattern(then)
            );
            tx.commit();

            String jsonConceptId = rule.getId().getValue();
            String jsonRuleLabel = rule.getLabel().getValue();
            String jsonRuleWhen = rule.getWhen().toString();
            String jsonRuleThen = rule.getThen().toString();
            LOG.debug("postRule - rule " + jsonRuleLabel + " with id " + jsonConceptId + " added. request processed.");
            response.status(HttpStatus.SC_OK);
            Json responseBody = ruleJson(jsonConceptId, jsonRuleLabel, jsonRuleWhen, jsonRuleThen);

            return responseBody;
        }
    }

    private Json ruleJson(String conceptId, String label, String when, String then) {
        return Json.object(
            RULE_OBJECT_JSON_FIELD, Json.object(
                CONCEPT_ID_JSON_FIELD, conceptId,
                LABEL_JSON_FIELD, label,
                WHEN_JSON_FIELD, when,
                THEN_JSON_FIELD, then
            )
        );
    }
}
