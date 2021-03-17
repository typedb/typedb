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
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;
import grakn.core.server.TransactionService;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto.Transaction;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.regex.Pattern;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static grakn.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static grakn.core.common.exception.ErrorMessage.Server.MISSING_FIELD;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ILLEGAL_SUPERTYPE_ENCODING;
import static grakn.core.server.common.ResponseBuilder.Type.createRes;
import static grakn.core.server.common.ResponseBuilder.Type.deleteRes;
import static grakn.core.server.common.ResponseBuilder.Type.getInstancesResPart;
import static grakn.core.server.common.ResponseBuilder.Type.getOwnersResPart;
import static grakn.core.server.common.ResponseBuilder.Type.getOwnsResPart;
import static grakn.core.server.common.ResponseBuilder.Type.getPlayersResPart;
import static grakn.core.server.common.ResponseBuilder.Type.getPlaysResPart;
import static grakn.core.server.common.ResponseBuilder.Type.getRegexRes;
import static grakn.core.server.common.ResponseBuilder.Type.getRelatesForRoleLabelRes;
import static grakn.core.server.common.ResponseBuilder.Type.getRelatesResPart;
import static grakn.core.server.common.ResponseBuilder.Type.getRelationTypesResPart;
import static grakn.core.server.common.ResponseBuilder.Type.getRes;
import static grakn.core.server.common.ResponseBuilder.Type.getSubtypesResPart;
import static grakn.core.server.common.ResponseBuilder.Type.getSupertypeRes;
import static grakn.core.server.common.ResponseBuilder.Type.getSupertypesResPart;
import static grakn.core.server.common.ResponseBuilder.Type.isAbstractRes;
import static grakn.core.server.common.ResponseBuilder.Type.putRes;
import static grakn.core.server.common.ResponseBuilder.Type.setAbstractRes;
import static grakn.core.server.common.ResponseBuilder.Type.setLabelRes;
import static grakn.core.server.common.ResponseBuilder.Type.setOwnsRes;
import static grakn.core.server.common.ResponseBuilder.Type.setPlaysRes;
import static grakn.core.server.common.ResponseBuilder.Type.setRegexRes;
import static grakn.core.server.common.ResponseBuilder.Type.setRelatesRes;
import static grakn.core.server.common.ResponseBuilder.Type.setSupertypeRes;
import static grakn.core.server.common.ResponseBuilder.Type.unsetAbstractRes;
import static grakn.core.server.common.ResponseBuilder.Type.unsetOwnsRes;
import static grakn.core.server.common.ResponseBuilder.Type.unsetPlaysRes;
import static grakn.core.server.common.ResponseBuilder.Type.unsetRelatesRes;
import static grakn.core.server.common.ResponseBuilder.Type.valueType;
import static grakn.protocol.ConceptProto.RelationType.SetRelates.Req.OverriddenCase.OVERRIDDEN_LABEL;
import static grakn.protocol.ConceptProto.ThingType.GetOwns.Req.FilterCase.VALUE_TYPE;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;

public class TypeService {

    private final TransactionService transactionSrv;
    private final ConceptManager conceptMgr;

    public TypeService(TransactionService transactionSrv, ConceptManager conceptMgr) {
        this.transactionSrv = transactionSrv;
        this.conceptMgr = conceptMgr;
    }

    public void execute(Transaction.Req request) {
        ConceptProto.Type.Req typeReq = request.getTypeReq();
        String label = typeReq.getLabel();
        String scope = typeReq.getScope();
        Type type = scope != null && !scope.isEmpty() ?
                notNull(conceptMgr.getRelationType(scope)).getRelates(label) :
                notNull(conceptMgr.getThingType(label));
        switch (typeReq.getReqCase()) {
            case TYPE_DELETE_REQ:
                delete(type, request);
                return;
            case TYPE_SET_LABEL_REQ:
                setLabel(type, typeReq.getTypeSetLabelReq().getLabel(), request);
                return;
            case TYPE_IS_ABSTRACT_REQ:
                isAbstract(type, request);
                return;
            case TYPE_GET_SUPERTYPE_REQ:
                getSupertype(type, request);
                return;
            case TYPE_SET_SUPERTYPE_REQ:
                setSupertype(type, typeReq.getTypeSetSupertypeReq().getType(), request);
                return;
            case TYPE_GET_SUPERTYPES_REQ:
                getSupertypes(type, request);
                return;
            case TYPE_GET_SUBTYPES_REQ:
                getSubtypes(type, request);
                return;
            case ROLE_TYPE_GET_RELATION_TYPES_REQ:
                getRelationTypes(type.asRoleType(), request);
                return;
            case ROLE_TYPE_GET_PLAYERS_REQ:
                getPlayers(type.asRoleType(), request);
                return;
            case THING_TYPE_SET_ABSTRACT_REQ:
                setAbstract(type.asThingType(), request);
                return;
            case THING_TYPE_UNSET_ABSTRACT_REQ:
                unsetAbstract(type.asThingType(), request);
                return;
            case THING_TYPE_SET_OWNS_REQ:
                setOwns(type.asThingType(), typeReq.getThingTypeSetOwnsReq(), request);
                return;
            case THING_TYPE_SET_PLAYS_REQ:
                setPlays(type.asThingType(), typeReq.getThingTypeSetPlaysReq(), request);
                return;
            case THING_TYPE_UNSET_OWNS_REQ:
                unsetOwns(type.asThingType(), typeReq.getThingTypeUnsetOwnsReq().getAttributeType(), request);
                return;
            case THING_TYPE_UNSET_PLAYS_REQ:
                unsetPlays(type.asThingType(), typeReq.getThingTypeUnsetPlaysReq().getRole(), request);
                return;
            case THING_TYPE_GET_INSTANCES_REQ:
                getInstances(type.asThingType(), request);
                return;
            case THING_TYPE_GET_OWNS_REQ:
                getOwns(type.asThingType(), typeReq, request);
                return;
            case THING_TYPE_GET_PLAYS_REQ:
                getPlays(type.asThingType(), request);
                return;
            case ENTITY_TYPE_CREATE_REQ:
                create(type.asEntityType(), request);
                return;
            case RELATION_TYPE_CREATE_REQ:
                create(type.asRelationType(), request);
                return;
            case RELATION_TYPE_GET_RELATES_FOR_ROLE_LABEL_REQ:
                String roleLabel = typeReq.getRelationTypeGetRelatesForRoleLabelReq().getLabel();
                getRelatesForRoleLabel(type.asRelationType(), roleLabel, request);
                return;
            case RELATION_TYPE_SET_RELATES_REQ:
                setRelates(type.asRelationType(), typeReq.getRelationTypeSetRelatesReq(), request);
                return;
            case RELATION_TYPE_UNSET_RELATES_REQ:
                unsetRelates(type.asRelationType(), typeReq.getRelationTypeUnsetRelatesReq(), request);
                return;
            case RELATION_TYPE_GET_RELATES_REQ:
                getRelates(type.asRelationType(), request);
                return;
            case ATTRIBUTE_TYPE_PUT_REQ:
                put(type.asAttributeType(), typeReq.getAttributeTypePutReq().getValue(), request);
                return;
            case ATTRIBUTE_TYPE_GET_REQ:
                get(type.asAttributeType(), typeReq.getAttributeTypeGetReq().getValue(), request);
                return;
            case ATTRIBUTE_TYPE_GET_REGEX_REQ:
                getRegex(type.asAttributeType(), request);
                return;
            case ATTRIBUTE_TYPE_SET_REGEX_REQ:
                setRegex(type.asAttributeType(), typeReq.getAttributeTypeSetRegexReq().getRegex(), request);
                return;
            case ATTRIBUTE_TYPE_GET_OWNERS_REQ:
                getOwners(type.asAttributeType(), typeReq.getAttributeTypeGetOwnersReq().getOnlyKey(), request);
                return;
            case REQ_NOT_SET:
            default:
                throw GraknException.of(UNKNOWN_REQUEST_TYPE);
        }
    }

    private static <T extends Type> T notNull(@Nullable T type) {
        if (type == null) throw GraknException.of(MISSING_CONCEPT);
        return type;
    }

    private Type getThingType(ConceptProto.Type protoType) {
        return conceptMgr.getThingType(protoType.getLabel());
    }

    private RoleType getRoleType(ConceptProto.Type protoRoleType) {
        RelationType relationType = conceptMgr.getRelationType(protoRoleType.getScope());
        if (relationType != null) return relationType.getRelates(protoRoleType.getLabel());
        else return null;
    }

    private void setLabel(Type type, String label, Transaction.Req request) {
        type.setLabel(label);
        transactionSrv.respond(setLabelRes(request.getReqId()));
    }

    private void isAbstract(Type type, Transaction.Req request) {
        transactionSrv.respond(isAbstractRes(request.getReqId(), type.isAbstract()));
    }

    private void setAbstract(ThingType thingType, Transaction.Req req) {
        thingType.setAbstract();
        transactionSrv.respond(setAbstractRes(req.getReqId()));
    }

    private void unsetAbstract(ThingType thingType, Transaction.Req req) {
        thingType.unsetAbstract();
        transactionSrv.respond(unsetAbstractRes(req.getReqId()));
    }

    private void getSupertype(Type type, Transaction.Req request) {
        transactionSrv.respond(getSupertypeRes(request.getReqId(), type.getSupertype()));
    }

    private void setSupertype(Type type, ConceptProto.Type supertype, Transaction.Req req) {
        Type sup = getThingType(supertype);

        if (type.isEntityType()) {
            type.asEntityType().setSupertype(sup.asEntityType());
        } else if (type.isRelationType()) {
            type.asRelationType().setSupertype(sup.asRelationType());
        } else if (type.isAttributeType()) {
            type.asAttributeType().setSupertype(sup.asAttributeType());
        } else {
            throw GraknException.of(ILLEGAL_SUPERTYPE_ENCODING, className(type.getClass()));
        }

        transactionSrv.respond(setSupertypeRes(req.getReqId()));
    }

    private void getSupertypes(Type type, Transaction.Req req) {
        transactionSrv.stream(type.getSupertypes().iterator(), req.getReqId(),
                              types -> getSupertypesResPart(req.getReqId(), types));
    }

    private void getSubtypes(Type type, Transaction.Req req) {
        transactionSrv.stream(type.getSubtypes().iterator(), req.getReqId(),
                              types -> getSubtypesResPart(req.getReqId(), types));
    }

    private void getInstances(ThingType thingType, Transaction.Req req) {
        transactionSrv.stream(thingType.getInstances().iterator(), req.getReqId(),
                              things -> getInstancesResPart(req.getReqId(), things));
    }

    private void getOwns(ThingType thingType, ConceptProto.Type.Req typeReq, Transaction.Req req) {
        if (typeReq.getThingTypeGetOwnsReq().getFilterCase() == VALUE_TYPE) {
            getOwns(thingType, valueType(typeReq.getThingTypeGetOwnsReq().getValueType()),
                    typeReq.getThingTypeGetOwnsReq().getKeysOnly(), req);
        } else {
            getOwns(thingType, typeReq.getThingTypeGetOwnsReq().getKeysOnly(), req);
        }
    }

    private void getOwns(ThingType thingType, boolean keysOnly, Transaction.Req req) {
        transactionSrv.stream(thingType.getOwns(keysOnly).iterator(), req.getReqId(),
                              attributeTypes -> getOwnsResPart(req.getReqId(), attributeTypes));
    }

    private void getOwns(ThingType thingType, AttributeType.ValueType valueType,
                         boolean keysOnly, Transaction.Req req) {
        transactionSrv.stream(thingType.getOwns(valueType, keysOnly).iterator(), req.getReqId(),
                              attributeTypes -> getOwnsResPart(req.getReqId(), attributeTypes));
    }

    private void getPlays(ThingType thingType, Transaction.Req req) {
        transactionSrv.stream(thingType.getPlays().iterator(), req.getReqId(),
                              roleTypes -> getPlaysResPart(req.getReqId(), roleTypes));
    }

    private void setOwns(ThingType thingType, ConceptProto.ThingType.SetOwns.Req setOwnsRequest,
                         Transaction.Req req) {
        AttributeType attributeType = getThingType(setOwnsRequest.getAttributeType()).asAttributeType();
        boolean isKey = setOwnsRequest.getIsKey();

        if (setOwnsRequest.hasOverriddenType()) {
            AttributeType overriddenType = getThingType(setOwnsRequest.getOverriddenType()).asAttributeType();
            thingType.setOwns(attributeType, overriddenType, isKey);
        } else {
            thingType.setOwns(attributeType, isKey);
        }
        transactionSrv.respond(setOwnsRes(req.getReqId()));
    }

    private void setPlays(ThingType thingType, ConceptProto.ThingType.SetPlays.Req setPlaysRequest,
                          Transaction.Req req) {
        RoleType role = getRoleType(setPlaysRequest.getRole());
        if (setPlaysRequest.hasOverriddenRole()) {
            RoleType overriddenRole = getRoleType(setPlaysRequest.getOverriddenRole());
            thingType.setPlays(role, overriddenRole);
        } else {
            thingType.setPlays(role);
        }
        transactionSrv.respond(setPlaysRes(req.getReqId()));
    }

    private void unsetOwns(ThingType thingType, ConceptProto.Type protoAttributeType, Transaction.Req req) {
        thingType.unsetOwns(getThingType(protoAttributeType).asAttributeType());
        transactionSrv.respond(unsetOwnsRes(req.getReqId()));
    }

    private void unsetPlays(ThingType thingType, ConceptProto.Type protoRoleType, Transaction.Req req) {
        thingType.unsetPlays(notNull(getRoleType(protoRoleType)));
        transactionSrv.respond(unsetPlaysRes(req.getReqId()));
    }

    private void getOwners(AttributeType attributeType, boolean onlyKey, Transaction.Req req) {
        transactionSrv.stream(attributeType.getOwners(onlyKey).iterator(), req.getReqId(),
                              owners -> getOwnersResPart(req.getReqId(), owners));
    }

    private void put(AttributeType attributeType, ConceptProto.Attribute.Value protoValue, Transaction.Req req) {
        Attribute attribute;
        switch (protoValue.getValueCase()) {
            case STRING:
                attribute = attributeType.asString().put(protoValue.getString());
                break;
            case DOUBLE:
                attribute = attributeType.asDouble().put(protoValue.getDouble());
                break;
            case LONG:
                attribute = attributeType.asLong().put(protoValue.getLong());
                break;
            case DATE_TIME:
                LocalDateTime dateTime = ofEpochMilli(protoValue.getDateTime()).atOffset(UTC).toLocalDateTime();
                attribute = attributeType.asDateTime().put(dateTime);
                break;
            case BOOLEAN:
                attribute = attributeType.asBoolean().put(protoValue.getBoolean());
                break;
            case VALUE_NOT_SET:
                throw GraknException.of(MISSING_FIELD, "value");
            default:
                throw GraknException.of(BAD_VALUE_TYPE, protoValue.getValueCase());
        }

        transactionSrv.respond(putRes(req.getReqId(), attribute));
    }

    private void get(AttributeType attributeType, ConceptProto.Attribute.Value protoValue, Transaction.Req req) {
        Attribute attribute;
        switch (protoValue.getValueCase()) {
            case STRING:
                attribute = attributeType.asString().get(protoValue.getString());
                break;
            case DOUBLE:
                attribute = attributeType.asDouble().get(protoValue.getDouble());
                break;
            case LONG:
                attribute = attributeType.asLong().get(protoValue.getLong());
                break;
            case DATE_TIME:
                LocalDateTime dateTime = ofEpochMilli(protoValue.getDateTime()).atOffset(UTC).toLocalDateTime();
                attribute = attributeType.asDateTime().get(dateTime);
                break;
            case BOOLEAN:
                attribute = attributeType.asBoolean().get(protoValue.getBoolean());
                break;
            case VALUE_NOT_SET:
            default:
                // TODO: Unify our exceptions - they should either all be GraknException or all be StatusRuntimeException
                throw GraknException.of(BAD_VALUE_TYPE);
        }

        transactionSrv.respond(getRes(req.getReqId(), attribute));
    }

    private void getRegex(AttributeType attributeType, Transaction.Req req) {
        transactionSrv.respond(getRegexRes(req.getReqId(), attributeType.asString().getRegex()));
    }

    private void setRegex(AttributeType attributeType, String regex, Transaction.Req req) {
        if (regex.isEmpty()) attributeType.asString().setRegex(null);
        else attributeType.asString().setRegex(Pattern.compile(regex));
        transactionSrv.respond(setRegexRes(req.getReqId()));
    }

    private void getRelates(RelationType relationType, Transaction.Req req) {
        transactionSrv.stream(relationType.getRelates().iterator(), req.getReqId(),
                              roleTypes -> getRelatesResPart(req.getReqId(), roleTypes));
    }

    private void getRelatesForRoleLabel(RelationType relationType, String roleLabel, Transaction.Req req) {
        transactionSrv.respond(getRelatesForRoleLabelRes(req.getReqId(), relationType.getRelates(roleLabel)));
    }

    private void setRelates(RelationType relationType, ConceptProto.RelationType.SetRelates.Req setRelatesReq,
                            Transaction.Req req) {
        if (setRelatesReq.getOverriddenCase() == OVERRIDDEN_LABEL) {
            relationType.setRelates(setRelatesReq.getLabel(), setRelatesReq.getOverriddenLabel());
        } else {
            relationType.setRelates(setRelatesReq.getLabel());
        }
        transactionSrv.respond(setRelatesRes(req.getReqId()));
    }

    private void unsetRelates(RelationType relationType, ConceptProto.RelationType.UnsetRelates.Req unsetRelatesReq,
                              Transaction.Req req) {
        relationType.unsetRelates(unsetRelatesReq.getLabel());
        transactionSrv.respond(unsetRelatesRes(req.getReqId()));
    }

    private void getRelationTypes(RoleType roleType, Transaction.Req req) {
        transactionSrv.stream(roleType.getRelationTypes().iterator(), req.getReqId(),
                              relationTypes -> getRelationTypesResPart(req.getReqId(), relationTypes));
    }

    private void getPlayers(RoleType roleType, Transaction.Req req) {
        transactionSrv.stream(roleType.getPlayers().iterator(), req.getReqId(),
                              players -> getPlayersResPart(req.getReqId(), players));
    }

    private void create(EntityType entityType, Transaction.Req req) {
        transactionSrv.respond(createRes(req.getReqId(), entityType.create()));
    }

    private void create(RelationType relationType, Transaction.Req req) {
        transactionSrv.respond(createRes(req.getReqId(), relationType.create()));
    }

    private void delete(Type type, Transaction.Req request) {
        type.delete();
        transactionSrv.respond(deleteRes(request.getReqId()));
    }

}
