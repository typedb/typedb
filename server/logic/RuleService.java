/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.server.logic;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.logic.LogicManager;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.server.TransactionService;
import com.vaticle.typedb.protocol.LogicProto;
import com.vaticle.typedb.protocol.TransactionProto.Transaction;

import javax.annotation.Nullable;
import java.util.UUID;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static com.vaticle.typedb.core.server.common.RequestReader.byteStringAsUUID;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Logic.Rule.deleteRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Logic.Rule.setLabelRes;

public class RuleService {

    private final TransactionService transactionSvc;
    private final LogicManager logicMgr;

    public RuleService(TransactionService transactionSvc, LogicManager logicMgr) {
        this.transactionSvc = transactionSvc;
        this.logicMgr = logicMgr;
    }

    public void execute(Transaction.Req req) {
        LogicProto.Rule.Req ruleReq = req.getRuleReq();
        Rule rule = notNull(logicMgr.getRule(ruleReq.getLabel()));
        UUID reqID = byteStringAsUUID(req.getReqId());
        switch (ruleReq.getReqCase()) {
            case RULE_DELETE_REQ:
                delete(rule, reqID);
                return;
            case RULE_SET_LABEL_REQ:
                setLabel(rule, ruleReq.getRuleSetLabelReq().getLabel(), reqID);
                return;
            case REQ_NOT_SET:
            default:
                throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
        }
    }

    private void setLabel(Rule rule, String label, UUID reqID) {
        rule.setLabel(label);
        transactionSvc.respond(setLabelRes(reqID));
    }

    private void delete(Rule rule, UUID reqID) {
        rule.delete();
        transactionSvc.respond(deleteRes(reqID));
    }

    private static Rule notNull(@Nullable Rule rule) {
        if (rule == null) throw TypeDBException.of(MISSING_CONCEPT);
        return rule;
    }
}
