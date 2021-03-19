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

package grakn.core.server.concept;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.server.TransactionService;
import grakn.core.server.common.ResponseBuilder;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto.Transaction;

import static grakn.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.server.common.ResponseBuilder.ConceptManager.getThingRes;
import static grakn.core.server.common.ResponseBuilder.ConceptManager.putAttributeTypeRes;
import static grakn.core.server.common.ResponseBuilder.ConceptManager.putEntityTypeRes;
import static grakn.core.server.common.ResponseBuilder.ConceptManager.putRelationTypeRes;

public class ConceptService {

    private final TransactionService transactionSvc;
    private final ConceptManager conceptMgr;

    public ConceptService(TransactionService transactionSvc, ConceptManager conceptMgr) {
        this.transactionSvc = transactionSvc;
        this.conceptMgr = conceptMgr;
    }

    public void execute(Transaction.Req req) {
        ConceptProto.ConceptManager.Req conceptMgrReq = req.getConceptManagerReq();
        switch (conceptMgrReq.getReqCase()) {
            case GET_THING_TYPE_REQ:
                getThingType(conceptMgrReq.getGetThingTypeReq().getLabel(), req);
                return;
            case GET_THING_REQ:
                getThing(conceptMgrReq.getGetThingReq().getIid().toByteArray(), req);
                return;
            case PUT_ENTITY_TYPE_REQ:
                putEntityType(conceptMgrReq.getPutEntityTypeReq().getLabel(), req);
                return;
            case PUT_ATTRIBUTE_TYPE_REQ:
                putAttributeType(conceptMgrReq.getPutAttributeTypeReq(), req);
                return;
            case PUT_RELATION_TYPE_REQ:
                putRelationType(conceptMgrReq.getPutRelationTypeReq().getLabel(), req);
                return;
            default:
            case REQ_NOT_SET:
                throw GraknException.of(UNKNOWN_REQUEST_TYPE);
        }
    }

    private void getThingType(String label, Transaction.Req req) {
        transactionSvc.respond(ResponseBuilder.ConceptManager.getThingTypeRes(req.getReqId(), conceptMgr.getThingType(label)));
    }

    private void getThing(byte[] iid, Transaction.Req req) {
        transactionSvc.respond(getThingRes(req.getReqId(), conceptMgr.getThing(iid)));
    }

    private void putEntityType(String label, Transaction.Req req) {
        EntityType entityType = conceptMgr.putEntityType(label);
        transactionSvc.respond(putEntityTypeRes(req.getReqId(), entityType));
    }

    private void putAttributeType(ConceptProto.ConceptManager.PutAttributeType.Req attributeTypeReq, Transaction.Req req) {
        ConceptProto.AttributeType.ValueType valueTypeProto = attributeTypeReq.getValueType();
        AttributeType.ValueType valueType;
        switch (valueTypeProto) {
            case STRING:
                valueType = AttributeType.ValueType.STRING;
                break;
            case BOOLEAN:
                valueType = AttributeType.ValueType.BOOLEAN;
                break;
            case LONG:
                valueType = AttributeType.ValueType.LONG;
                break;
            case DOUBLE:
                valueType = AttributeType.ValueType.DOUBLE;
                break;
            case DATETIME:
                valueType = AttributeType.ValueType.DATETIME;
                break;
            case OBJECT:
            case UNRECOGNIZED:
            default:
                throw GraknException.of(BAD_VALUE_TYPE, valueTypeProto);
        }
        AttributeType attributeType = conceptMgr.putAttributeType(attributeTypeReq.getLabel(), valueType);
        transactionSvc.respond(putAttributeTypeRes(req.getReqId(), attributeType));
    }

    private void putRelationType(String label, Transaction.Req req) {
        RelationType relationType = conceptMgr.putRelationType(label);
        String reqID = req.getReqId();
        transactionSvc.respond(putRelationTypeRes(reqID, relationType));
    }

}
