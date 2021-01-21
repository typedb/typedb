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

package grakn.core.server.rpc.logic;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.server.rpc.TransactionRPC;
import grakn.core.server.rpc.util.ResponseBuilder;
import grakn.protocol.LogicProto;
import grakn.protocol.TransactionProto;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.pattern.variable.ThingVariable;

import static grakn.core.server.rpc.util.ResponseBuilder.Logic.rule;
import static java.util.stream.Collectors.toList;


public class LogicManagerHandler {

    private final TransactionRPC transactionRPC;
    private final LogicManager logicManager;

    public LogicManagerHandler(TransactionRPC transactionRPC, LogicManager logicManager) {
        this.transactionRPC = transactionRPC;
        this.logicManager = logicManager;
    }


    public void handleRequest(TransactionProto.Transaction.Req request) {
        LogicProto.LogicManager.Req logicManagerReq = request.getLogicManagerReq();
        switch (logicManagerReq.getReqCase()) {
            case GET_RULE_REQ:
                getRule(request, logicManagerReq.getGetRuleReq().getLabel());
                return;
            case PUT_RULE_REQ:
                putRule(request, logicManagerReq.getPutRuleReq());
                return;
            case GET_RULES_REQ:
                getRules(request);
                return;
            default:
            case REQ_NOT_SET:
                throw GraknException.of(ErrorMessage.Server.UNKNOWN_REQUEST_TYPE);
        }
    }

    private static TransactionProto.Transaction.Res response(TransactionProto.Transaction.Req request, LogicProto.LogicManager.Res.Builder res) {
        return TransactionProto.Transaction.Res.newBuilder().setId(request.getId()).setLogicManagerRes(res).build();
    }

    private void getRule(TransactionProto.Transaction.Req request, String label) {
        final Rule rule = logicManager.getRule(label);
        final LogicProto.LogicManager.GetRule.Res.Builder getRuleRes = LogicProto.LogicManager.GetRule.Res.newBuilder();
        if (rule != null) getRuleRes.setRule(rule(rule));
        transactionRPC.respond(response(request, LogicProto.LogicManager.Res.newBuilder().setGetRuleRes(getRuleRes)));
    }

    private void putRule(TransactionProto.Transaction.Req request, LogicProto.LogicManager.PutRule.Req req) {
        final Conjunction<? extends Pattern> when = Graql.parsePattern(req.getWhen()).asConjunction();
        final ThingVariable<?> then = Graql.parseVariable(req.getThen()).asThing();
        final Rule rule = logicManager.putRule(req.getLabel(), when, then);
        final LogicProto.LogicManager.Res.Builder res = LogicProto.LogicManager.Res.newBuilder()
                .setPutRuleRes(LogicProto.LogicManager.PutRule.Res.newBuilder().setRule(rule(rule)));
        transactionRPC.respond(response(request, res));
    }

    private void getRules(TransactionProto.Transaction.Req request) {
        final ResourceIterator<Rule> rules = logicManager.rules();
        transactionRPC.respond(request, rules,
                as -> response(request, LogicProto.LogicManager.Res.newBuilder().setGetRulesRes(
                        LogicProto.LogicManager.GetRules.Res.newBuilder()
                                .addAllRules(as.stream().map(ResponseBuilder.Logic::rule).collect(toList())))));
    }
}
