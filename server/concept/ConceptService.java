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

package com.vaticle.typedb.core.server.concept;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.server.TransactionService;
import com.vaticle.typedb.core.server.common.ResponseBuilder;
import com.vaticle.typedb.protocol.ConceptProto;
import com.vaticle.typedb.protocol.TransactionProto.Transaction;

import java.util.UUID;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static com.vaticle.typedb.core.server.common.RequestReader.byteStringAsUUID;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.ConceptManager.getThingRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.ConceptManager.putAttributeTypeRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.ConceptManager.putEntityTypeRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.ConceptManager.putRelationTypeRes;

public class ConceptService {

    private final TransactionService transactionSvc;
    private final ConceptManager conceptMgr;

    public ConceptService(TransactionService transactionSvc, ConceptManager conceptMgr) {
        this.transactionSvc = transactionSvc;
        this.conceptMgr = conceptMgr;
    }

    public void execute(Transaction.Req req) {
        ConceptProto.ConceptManager.Req conceptMgrReq = req.getConceptManagerReq();
        UUID reqID = byteStringAsUUID(req.getReqId());
        switch (conceptMgrReq.getReqCase()) {
            case GET_THING_TYPE_REQ:
                getThingType(conceptMgrReq.getGetThingTypeReq().getLabel(), reqID);
                return;
            case GET_THING_REQ:
                getThing(ByteArray.of(conceptMgrReq.getGetThingReq().getIid().toByteArray()), reqID);
                return;
            case PUT_ENTITY_TYPE_REQ:
                putEntityType(conceptMgrReq.getPutEntityTypeReq().getLabel(), reqID);
                return;
            case PUT_ATTRIBUTE_TYPE_REQ:
                putAttributeType(conceptMgrReq.getPutAttributeTypeReq(), reqID);
                return;
            case PUT_RELATION_TYPE_REQ:
                putRelationType(conceptMgrReq.getPutRelationTypeReq().getLabel(), reqID);
                return;
            default:
            case REQ_NOT_SET:
                throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
        }
    }

    private void getThingType(String label, UUID reqID) {
        transactionSvc.respond(ResponseBuilder.ConceptManager.getThingTypeRes(reqID, conceptMgr.getThingType(label)));
    }

    private void getThing(ByteArray iid, UUID reqID) {
        transactionSvc.respond(getThingRes(reqID, conceptMgr.getThing(iid)));
    }

    private void putEntityType(String label, UUID reqID) {
        EntityType entityType = conceptMgr.putEntityType(label);
        transactionSvc.respond(putEntityTypeRes(reqID, entityType));
    }

    private void putAttributeType(ConceptProto.ConceptManager.PutAttributeType.Req attributeTypeReq, UUID reqID) {
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
                throw TypeDBException.of(BAD_VALUE_TYPE, valueTypeProto);
        }
        AttributeType attributeType = conceptMgr.putAttributeType(attributeTypeReq.getLabel(), valueType);
        transactionSvc.respond(putAttributeTypeRes(reqID, attributeType));
    }

    private void putRelationType(String label, UUID reqID) {
        RelationType relationType = conceptMgr.putRelationType(label);
        transactionSvc.respond(putRelationTypeRes(reqID, relationType));
    }

}
