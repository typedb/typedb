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

package grakn.core.server.database.concept;

import grakn.core.common.exception.GraknException;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.ThingType;
import grakn.core.server.database.transaction.TransactionService;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto;
import grakn.protocol.TransactionProto.Transaction;

import static grakn.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.server.database.common.ResponseBuilder.Concept.thing;
import static grakn.core.server.database.common.ResponseBuilder.Concept.type;

public class ConceptManagerService {

    private final TransactionService transactionSrv;
    private final ConceptManager conceptMgr;

    public ConceptManagerService(TransactionService transactionSrv, ConceptManager conceptMgr) {
        this.transactionSrv = transactionSrv;
        this.conceptMgr = conceptMgr;
    }

    public void execute(Transaction.Req request) {
        ConceptProto.ConceptManager.Req conceptMgrReq = request.getConceptManagerReq();
        switch (conceptMgrReq.getReqCase()) {
            case GET_THING_TYPE_REQ:
                getThingType(conceptMgrReq.getGetThingTypeReq().getLabel(), request);
                return;
            case GET_THING_REQ:
                getThing(conceptMgrReq.getGetThingReq().getIid().toByteArray(), request);
                return;
            case PUT_ENTITY_TYPE_REQ:
                putEntityType(conceptMgrReq.getPutEntityTypeReq().getLabel(), request);
                return;
            case PUT_ATTRIBUTE_TYPE_REQ:
                putAttributeType(conceptMgrReq.getPutAttributeTypeReq(), request);
                return;
            case PUT_RELATION_TYPE_REQ:
                putRelationType(conceptMgrReq.getPutRelationTypeReq().getLabel(), request);
                return;
            default:
            case REQ_NOT_SET:
                throw GraknException.of(UNKNOWN_REQUEST_TYPE);
        }
    }

    private static TransactionProto.Transaction.Res response(Transaction.Req request,
                                                             ConceptProto.ConceptManager.Res.Builder response) {
        return TransactionProto.Transaction.Res.newBuilder().setId(request.getId())
                .setConceptManagerRes(response).build();
    }

    private void getThingType(String label, Transaction.Req request) {
        ThingType thingType = conceptMgr.getThingType(label);
        ConceptProto.ConceptManager.GetThingType.Res.Builder getThingTypeRes =
                ConceptProto.ConceptManager.GetThingType.Res.newBuilder();
        if (thingType != null) getThingTypeRes.setThingType(type(thingType));
        ConceptProto.ConceptManager.Res.Builder response =
                ConceptProto.ConceptManager.Res.newBuilder().setGetThingTypeRes(getThingTypeRes);
        transactionSrv.respond(response(request, response));
    }

    private void getThing(byte[] iid, Transaction.Req request) {
        Thing thing = conceptMgr.getThing(iid);
        ConceptProto.ConceptManager.GetThing.Res.Builder getThingRes =
                ConceptProto.ConceptManager.GetThing.Res.newBuilder();
        if (thing != null) getThingRes.setThing(thing(thing));
        ConceptProto.ConceptManager.Res.Builder response =
                ConceptProto.ConceptManager.Res.newBuilder().setGetThingRes(getThingRes);
        transactionSrv.respond(response(request, response));
    }

    private void putEntityType(String label, Transaction.Req request) {
        EntityType entityType = conceptMgr.putEntityType(label);
        transactionSrv.respond(response(request, ConceptProto.ConceptManager.Res.newBuilder().setPutEntityTypeRes(
                ConceptProto.ConceptManager.PutEntityType.Res.newBuilder().setEntityType(type(entityType))
        )));
    }

    private void putAttributeType(ConceptProto.ConceptManager.PutAttributeType.Req attributeTypeReq, Transaction.Req request) {
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
        transactionSrv.respond(response(request, ConceptProto.ConceptManager.Res.newBuilder().setPutAttributeTypeRes(
                ConceptProto.ConceptManager.PutAttributeType.Res.newBuilder().setAttributeType(type(attributeType))
        )));
    }

    private void putRelationType(String label, Transaction.Req request) {
        RelationType relationType = conceptMgr.putRelationType(label);
        transactionSrv.respond(response(request, ConceptProto.ConceptManager.Res.newBuilder().setPutRelationTypeRes(
                ConceptProto.ConceptManager.PutRelationType.Res.newBuilder().setRelationType(type(relationType))
        )));
    }

}
