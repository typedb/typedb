/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.server.rpc.concept;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.logic.Rule;
import grakn.core.server.rpc.TransactionRPC;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto;
import grakn.protocol.TransactionProto.Transaction;

import javax.annotation.Nullable;

import static grakn.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;

public class RuleHandler {

    private final TransactionRPC transactionRPC;
    private final ConceptManager conceptManager;

    public RuleHandler(TransactionRPC transactionRPC, ConceptManager conceptManager) {
        this.transactionRPC = transactionRPC;
        this.conceptManager = conceptManager;
    }

    public void handleRequest(Transaction.Req request) {
        final ConceptProto.Rule.Req ruleReq = request.getRuleReq();
        final Rule rule = notNull(conceptManager.getRule(ruleReq.getLabel()));
        switch (ruleReq.getReqCase()) {
            case RULE_DELETE_REQ:
                delete(request, rule);
                return;
            case RULE_SET_LABEL_REQ:
                setLabel(request, rule, ruleReq.getRuleSetLabelReq().getLabel());
                return;
            case REQ_NOT_SET:
            default:
                throw new GraknException(UNKNOWN_REQUEST_TYPE);
        }
    }

    private static TransactionProto.Transaction.Res response(Transaction.Req request, ConceptProto.Rule.Res.Builder response) {
        return TransactionProto.Transaction.Res.newBuilder().setId(request.getId()).setRuleRes(response).build();
    }

    private static Rule notNull(@Nullable Rule rule) {
        if (rule == null) throw new GraknException(MISSING_CONCEPT);
        return rule;
    }

    private void delete(Transaction.Req request, Rule rule) {
        rule.delete();
        transactionRPC.respond(response(request, ConceptProto.Rule.Res.newBuilder().setRuleDeleteRes(ConceptProto.Rule.Delete.Res.getDefaultInstance())));
    }

    private void setLabel(Transaction.Req request, Rule rule, String label) {
        rule.setLabel(label);
        transactionRPC.respond(response(request, ConceptProto.Rule.Res.newBuilder().setRuleSetLabelRes(ConceptProto.Rule.SetLabel.Res.getDefaultInstance())));
    }
}
