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

import grakn.core.common.exception.GraknException;
import grakn.core.logic.LogicManager;
import grakn.core.logic.Rule;
import grakn.core.server.TransactionService;
import grakn.protocol.LogicProto;
import grakn.protocol.TransactionProto;
import grakn.protocol.TransactionProto.Transaction;

import javax.annotation.Nullable;

import static grakn.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;

public class RuleService {

    private final TransactionService transactionSrv;
    private final LogicManager logicMgr;

    public RuleService(TransactionService transactionSrv, LogicManager logicMgr) {
        this.transactionSrv = transactionSrv;
        this.logicMgr = logicMgr;
    }

    public void execute(Transaction.Req request) {
        LogicProto.Rule.Req ruleReq = request.getRuleReq();
        Rule rule = notNull(logicMgr.getRule(ruleReq.getLabel()));
        switch (ruleReq.getReqCase()) {
            case RULE_DELETE_REQ:
                delete(rule, request);
                return;
            case RULE_SET_LABEL_REQ:
                setLabel(rule, ruleReq.getRuleSetLabelReq().getLabel(), request);
                return;
            case REQ_NOT_SET:
            default:
                throw GraknException.of(UNKNOWN_REQUEST_TYPE);
        }
    }

    private static TransactionProto.Transaction.Res response(Transaction.Req request, LogicProto.Rule.Res.Builder response) {
        return TransactionProto.Transaction.Res.newBuilder().setId(request.getId()).setRuleRes(response).build();
    }

    private static Rule notNull(@Nullable Rule rule) {
        if (rule == null) throw GraknException.of(MISSING_CONCEPT);
        return rule;
    }

    private void delete(Rule rule, Transaction.Req request) {
        rule.delete();
        LogicProto.Rule.Res.Builder response =
                LogicProto.Rule.Res.newBuilder().setRuleDeleteRes(LogicProto.Rule.Delete.Res.getDefaultInstance());
        transactionSrv.respond(response(request, response));
    }

    private void setLabel(Rule rule, String label, Transaction.Req request) {
        rule.setLabel(label);
        LogicProto.Rule.Res.Builder response =
                LogicProto.Rule.Res.newBuilder().setRuleSetLabelRes(LogicProto.Rule.SetLabel.Res.getDefaultInstance());
        transactionSrv.respond(response(request, response));
    }
}
