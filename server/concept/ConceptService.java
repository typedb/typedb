/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.server.TransactionService;
import com.vaticle.typedb.core.server.common.ResponseBuilder;
import com.vaticle.typedb.protocol.ConceptProto;
import com.vaticle.typedb.protocol.TransactionProto.Transaction;

import java.util.EnumMap;
import java.util.UUID;
import java.util.function.BiConsumer;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static com.vaticle.typedb.core.server.common.RequestReader.byteStringAsUUID;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.ConceptManager.getAttributeRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.ConceptManager.getEntityRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.ConceptManager.getRelationRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.ConceptManager.getSchemaExceptionsRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.ConceptManager.putAttributeTypeRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.ConceptManager.putEntityTypeRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.ConceptManager.putRelationTypeRes;
import static com.vaticle.typedb.protocol.ConceptProto.ConceptManager.Req.ReqCase.GET_ATTRIBUTE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ConceptManager.Req.ReqCase.GET_ATTRIBUTE_TYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ConceptManager.Req.ReqCase.GET_ENTITY_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ConceptManager.Req.ReqCase.GET_ENTITY_TYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ConceptManager.Req.ReqCase.GET_RELATION_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ConceptManager.Req.ReqCase.GET_RELATION_TYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ConceptManager.Req.ReqCase.GET_SCHEMA_EXCEPTIONS_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ConceptManager.Req.ReqCase.PUT_ATTRIBUTE_TYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ConceptManager.Req.ReqCase.PUT_ENTITY_TYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ConceptManager.Req.ReqCase.PUT_RELATION_TYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ConceptManager.Req.ReqCase.REQ_NOT_SET;


public class ConceptService {

    private final TransactionService transactionSvc;
    private final ConceptManager conceptMgr;

    private final EnumMap<ConceptProto.ConceptManager.Req.ReqCase, BiConsumer<ConceptProto.ConceptManager.Req, UUID>> handlers;

    public ConceptService(TransactionService transactionSvc, ConceptManager conceptMgr) {
        this.transactionSvc = transactionSvc;
        this.conceptMgr = conceptMgr;

        handlers = new EnumMap<>(ConceptProto.ConceptManager.Req.ReqCase.class);
        handlers.put(GET_ENTITY_TYPE_REQ, this::getEntityType);
        handlers.put(GET_RELATION_TYPE_REQ, this::getRelationType);
        handlers.put(GET_ATTRIBUTE_TYPE_REQ, this::getAttributeType);
        handlers.put(PUT_ENTITY_TYPE_REQ, this::putEntityType);
        handlers.put(PUT_ATTRIBUTE_TYPE_REQ, this::putAttributeType);
        handlers.put(PUT_RELATION_TYPE_REQ, this::putRelationType);
        handlers.put(GET_ENTITY_REQ, this::getEntity);
        handlers.put(GET_RELATION_REQ, this::getRelation);
        handlers.put(GET_ATTRIBUTE_REQ, this::getAttribute);
        handlers.put(GET_SCHEMA_EXCEPTIONS_REQ, this::getSchemaExceptions);
        handlers.put(REQ_NOT_SET, this::requestNotSet);
        assert handlers.size() == ConceptProto.ConceptManager.Req.ReqCase.class.getEnumConstants().length;
    }

    public void execute(Transaction.Req req) {
        ConceptProto.ConceptManager.Req conceptMgrReq = req.getConceptManagerReq();
        UUID reqID = byteStringAsUUID(req.getReqId());
        if (!handlers.containsKey(conceptMgrReq.getReqCase())) throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
        handlers.get(conceptMgrReq.getReqCase()).accept(conceptMgrReq, reqID);
    }

    private void getEntityType(ConceptProto.ConceptManager.Req req, UUID reqID) {
        transactionSvc.respond(ResponseBuilder.ConceptManager.getEntityTypeRes(
                reqID, conceptMgr.getEntityType(req.getGetEntityTypeReq().getLabel())
        ));
    }

    private void getRelationType(ConceptProto.ConceptManager.Req req, UUID reqID) {
        transactionSvc.respond(ResponseBuilder.ConceptManager.getRelationTypeRes(
                reqID, conceptMgr.getRelationType(req.getGetRelationTypeReq().getLabel())
        ));
    }

    private void getAttributeType(ConceptProto.ConceptManager.Req req, UUID reqID) {
        transactionSvc.respond(ResponseBuilder.ConceptManager.getAttributeTypeRes(
                reqID, conceptMgr.getAttributeType(req.getGetAttributeTypeReq().getLabel())
        ));
    }

    private void putEntityType(ConceptProto.ConceptManager.Req req, UUID reqID) {
        EntityType entityType = conceptMgr.putEntityType(req.getPutEntityTypeReq().getLabel());
        transactionSvc.respond(putEntityTypeRes(reqID, entityType));
    }

    private void putRelationType(ConceptProto.ConceptManager.Req req, UUID reqID) {
        RelationType relationType = conceptMgr.putRelationType(req.getPutRelationTypeReq().getLabel());
        transactionSvc.respond(putRelationTypeRes(reqID, relationType));
    }

    private void putAttributeType(ConceptProto.ConceptManager.Req req, UUID reqID) {
        ConceptProto.ConceptManager.PutAttributeType.Req attributeTypeReq = req.getPutAttributeTypeReq();
        ConceptProto.ValueType valueTypeProto = attributeTypeReq.getValueType();
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

    private void getEntity(ConceptProto.ConceptManager.Req req, UUID reqID) {
        ByteArray iid = ByteArray.of(req.getGetEntityReq().getIid().toByteArray());
        transactionSvc.respond(getEntityRes(reqID, conceptMgr.getEntity(iid)));
    }

    private void getRelation(ConceptProto.ConceptManager.Req req, UUID reqID) {
        ByteArray iid = ByteArray.of(req.getGetRelationReq().getIid().toByteArray());
        transactionSvc.respond(getRelationRes(reqID, conceptMgr.getRelation(iid)));
    }

    private void getAttribute(ConceptProto.ConceptManager.Req req, UUID reqID) {
        ByteArray iid = ByteArray.of(req.getGetAttributeReq().getIid().toByteArray());
        transactionSvc.respond(getAttributeRes(reqID, conceptMgr.getAttribute(iid)));
    }

    private void getSchemaExceptions(ConceptProto.ConceptManager.Req req, UUID reqID) {
        transactionSvc.respond(getSchemaExceptionsRes(reqID, conceptMgr.getSchemaExceptions()));
    }

    private void requestNotSet(ConceptProto.ConceptManager.Req thingReq, UUID uuid) {
        throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
    }
}
