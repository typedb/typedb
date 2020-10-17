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

import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.schema.Rule;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto;

import javax.annotation.Nullable;
import java.util.function.Consumer;

import static grakn.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;

public class RuleRPC {

    private final Rule rule;
    private final Consumer<TransactionProto.Transaction.Res> responder;

    public RuleRPC(final Grakn.Transaction transaction, final String label, final Consumer<TransactionProto.Transaction.Res> responder) {
        this.responder = responder;
        this.rule = notNull(transaction.concepts().getRule(label));

    }

    private static TransactionProto.Transaction.Res response(final ConceptProto.RuleMethod.Res response) {
        return TransactionProto.Transaction.Res.newBuilder().setRuleMethodRes(response).build();
    }

    private static Rule notNull(@Nullable final Rule rule) {
        if (rule == null) throw new GraknException(MISSING_CONCEPT);
        return rule;
    }

    public void execute(final ConceptProto.RuleMethod.Req req) {
        switch (req.getReqCase()) {
            case RULE_DELETE_REQ:
                this.delete();
                return;
            case RULE_SETLABEL_REQ:
                this.setLabel(req.getRuleSetLabelReq().getLabel());
                return;
            case REQ_NOT_SET:
            default:
                throw new GraknException(UNKNOWN_REQUEST_TYPE);
        }
    }

    private void delete() {
        rule.delete();
        responder.accept(null);
    }

    private void setLabel(final String label) {
        rule.setLabel(label);
        responder.accept(null);
    }
}
