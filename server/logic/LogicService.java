/*
 * Copyright (C) 2021 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.logic;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.server.TransactionService;
import grakn.core.server.common.ResponseBuilder;
import grakn.protocol.LogicProto;
import grakn.protocol.TransactionProto;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

import java.util.UUID;

import static grakn.core.server.common.RequestReader.byteStringAsUUID;

public class LogicService {

    private final TransactionService transactionSvc;
    private final grakn.core.logic.LogicManager logicMgr;

    public LogicService(TransactionService transactionSvc, grakn.core.logic.LogicManager logicMgr) {
        this.transactionSvc = transactionSvc;
        this.logicMgr = logicMgr;
    }

    public void execute(TransactionProto.Transaction.Req req) {
        LogicProto.LogicManager.Req logicManagerReq = req.getLogicManagerReq();
        UUID reqID = byteStringAsUUID(req.getReqId());
        switch (logicManagerReq.getReqCase()) {
            case GET_RULE_REQ:
                getRule(logicManagerReq.getGetRuleReq().getLabel(), reqID);
                return;
            case PUT_RULE_REQ:
                putRule(logicManagerReq.getPutRuleReq(), reqID);
                return;
            case GET_RULES_REQ:
                getRules(reqID);
                return;
            default:
            case REQ_NOT_SET:
                throw GraknException.of(ErrorMessage.Server.UNKNOWN_REQUEST_TYPE);
        }
    }

    private void putRule(LogicProto.LogicManager.PutRule.Req ruleReq, UUID reqID) {
        Conjunction<? extends Pattern> when = Graql.parsePattern(ruleReq.getWhen()).asConjunction();
        ThingVariable<?> then = Graql.parseVariable(ruleReq.getThen()).asThing();
        grakn.core.logic.Rule rule = logicMgr.putRule(ruleReq.getLabel(), when, then);
        transactionSvc.respond(ResponseBuilder.LogicManager.putRuleRes(reqID, rule));
    }

    private void getRule(String label, UUID reqID) {
        grakn.core.logic.Rule rule = logicMgr.getRule(label);
        transactionSvc.respond(ResponseBuilder.LogicManager.getRuleRes(reqID, rule));
    }

    private void getRules(UUID reqID) {
        FunctionalIterator<grakn.core.logic.Rule> rules = logicMgr.rules();
        transactionSvc.stream(rules, reqID, r -> ResponseBuilder.LogicManager.getRulesResPart(reqID, r));
    }
}
