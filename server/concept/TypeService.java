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
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.server.TransactionService;
import com.vaticle.typedb.protocol.ConceptProto;
import com.vaticle.typedb.protocol.TransactionProto.Transaction;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.MISSING_FIELD;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeWrite.ILLEGAL_SUPERTYPE_ENCODING;
import static com.vaticle.typedb.core.server.common.RequestReader.byteStringAsUUID;
import static com.vaticle.typedb.core.server.common.RequestReader.valueType;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.AttributeType.getOwnersExplicitResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.AttributeType.getOwnersResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.AttributeType.getRegexRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.AttributeType.getRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.AttributeType.putRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.AttributeType.setRegexRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.EntityType.createRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.RelationType.createRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.RelationType.getRelatesForRoleLabelRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.RelationType.getRelatesResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.RelationType.setRelatesRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.RelationType.unsetRelatesRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.RoleType.getPlayersResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.RoleType.getRelationTypesResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.getInstancesExplicitResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.getInstancesResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.getOwnsExplicitResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.getOwnsOverriddenRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.getOwnsResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.getPlaysExplicitResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.getPlaysOverriddenRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.getPlaysResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.setAbstractRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.setOwnsRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.setPlaysRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.unsetAbstractRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.unsetOwnsRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.ThingType.unsetPlaysRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.deleteRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.getSubtypesExplicitResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.getSubtypesResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.getSupertypeRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.getSupertypesResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.isAbstractRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.setLabelRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Type.setSupertypeRes;
import static com.vaticle.typedb.protocol.ConceptProto.RelationType.SetRelates.Req.OverriddenCase.OVERRIDDEN_LABEL;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;

public class TypeService {

    private final TransactionService transactionSvc;
    private final ConceptManager conceptMgr;

    public TypeService(TransactionService transactionSvc, ConceptManager conceptMgr) {
        this.transactionSvc = transactionSvc;
        this.conceptMgr = conceptMgr;
    }

    public void execute(Transaction.Req req) {
        ConceptProto.Type.Req typeReq = req.getTypeReq();
        String label = typeReq.getLabel();
        String scope = typeReq.getScope();
        Type type = scope != null && !scope.isEmpty() ?
                notNull(conceptMgr.getRelationType(scope)).getRelates(label) :
                notNull(conceptMgr.getThingType(label));
        UUID reqID = byteStringAsUUID(req.getReqId());
        switch (typeReq.getReqCase()) {
            case TYPE_DELETE_REQ:
                delete(type, reqID);
                return;
            case TYPE_SET_LABEL_REQ:
                setLabel(type, typeReq.getTypeSetLabelReq().getLabel(), reqID);
                return;
            case TYPE_IS_ABSTRACT_REQ:
                isAbstract(type, reqID);
                return;
            case TYPE_GET_SUPERTYPE_REQ:
                getSupertype(type, reqID);
                return;
            case TYPE_SET_SUPERTYPE_REQ:
                setSupertype(type, typeReq.getTypeSetSupertypeReq().getType(), reqID);
                return;
            case TYPE_GET_SUPERTYPES_REQ:
                getSupertypes(type, reqID);
                return;
            case TYPE_GET_SUBTYPES_REQ:
                getSubtypes(type, reqID);
                return;
            case TYPE_GET_SUBTYPES_EXPLICIT_REQ:
                getSubtypesExplicit(type, reqID);
                return;
            case ROLE_TYPE_GET_RELATION_TYPES_REQ:
                getRelationTypes(type.asRoleType(), reqID);
                return;
            case ROLE_TYPE_GET_PLAYERS_REQ:
                getPlayers(type.asRoleType(), reqID);
                return;
            case THING_TYPE_SET_ABSTRACT_REQ:
                setAbstract(type.asThingType(), reqID);
                return;
            case THING_TYPE_UNSET_ABSTRACT_REQ:
                unsetAbstract(type.asThingType(), reqID);
                return;
            case THING_TYPE_SET_OWNS_REQ:
                setOwns(type.asThingType(), typeReq.getThingTypeSetOwnsReq(), reqID);
                return;
            case THING_TYPE_SET_PLAYS_REQ:
                setPlays(type.asThingType(), typeReq.getThingTypeSetPlaysReq(), reqID);
                return;
            case THING_TYPE_UNSET_OWNS_REQ:
                unsetOwns(type.asThingType(), typeReq.getThingTypeUnsetOwnsReq().getAttributeType(), reqID);
                return;
            case THING_TYPE_UNSET_PLAYS_REQ:
                unsetPlays(type.asThingType(), typeReq.getThingTypeUnsetPlaysReq().getRole(), reqID);
                return;
            case THING_TYPE_GET_INSTANCES_REQ:
                getInstances(type.asThingType(), reqID);
                return;
            case THING_TYPE_GET_INSTANCES_EXPLICIT_REQ:
                getInstancesExplicit(type.asThingType(), reqID);
                return;
            case THING_TYPE_GET_OWNS_REQ:
                getOwns(type.asThingType(), typeReq.getThingTypeGetOwnsReq(), reqID);
                return;
            case THING_TYPE_GET_OWNS_EXPLICIT_REQ:
                getOwnsExplicit(type.asThingType(), typeReq.getThingTypeGetOwnsExplicitReq(), reqID);
                return;
            case THING_TYPE_GET_OWNS_OVERRIDDEN_REQ:
                getOwnsOverridden(type.asThingType(), typeReq.getThingTypeGetOwnsOverriddenReq(), reqID);
                return;
            case THING_TYPE_GET_PLAYS_REQ:
                getPlays(type.asThingType(), reqID);
                return;
            case THING_TYPE_GET_PLAYS_EXPLICIT_REQ:
                getPlaysExplicit(type.asThingType(), reqID);
                return;
            case THING_TYPE_GET_PLAYS_OVERRIDDEN_REQ:
                getPlaysOverridden(type.asThingType(), typeReq.getThingTypeGetPlaysOverriddenReq(), reqID);
                return;
            case ENTITY_TYPE_CREATE_REQ:
                create(type.asEntityType(), reqID);
                return;
            case RELATION_TYPE_CREATE_REQ:
                create(type.asRelationType(), reqID);
                return;
            case RELATION_TYPE_GET_RELATES_FOR_ROLE_LABEL_REQ:
                String roleLabel = typeReq.getRelationTypeGetRelatesForRoleLabelReq().getLabel();
                getRelatesForRoleLabel(type.asRelationType(), roleLabel, reqID);
                return;
            case RELATION_TYPE_SET_RELATES_REQ:
                setRelates(type.asRelationType(), typeReq.getRelationTypeSetRelatesReq(), reqID);
                return;
            case RELATION_TYPE_UNSET_RELATES_REQ:
                unsetRelates(type.asRelationType(), typeReq.getRelationTypeUnsetRelatesReq(), reqID);
                return;
            case RELATION_TYPE_GET_RELATES_REQ:
                getRelates(type.asRelationType(), reqID);
                return;
            case ATTRIBUTE_TYPE_PUT_REQ:
                put(type.asAttributeType(), typeReq.getAttributeTypePutReq().getValue(), reqID);
                return;
            case ATTRIBUTE_TYPE_GET_REQ:
                get(type.asAttributeType(), typeReq.getAttributeTypeGetReq().getValue(), reqID);
                return;
            case ATTRIBUTE_TYPE_GET_REGEX_REQ:
                getRegex(type.asAttributeType(), reqID);
                return;
            case ATTRIBUTE_TYPE_SET_REGEX_REQ:
                setRegex(type.asAttributeType(), typeReq.getAttributeTypeSetRegexReq().getRegex(), reqID);
                return;
            case ATTRIBUTE_TYPE_GET_OWNERS_REQ:
                getOwners(type.asAttributeType(), typeReq.getAttributeTypeGetOwnersReq().getOnlyKey(), reqID);
                return;
            case ATTRIBUTE_TYPE_GET_OWNERS_EXPLICIT_REQ:
                getOwnersExplicit(type.asAttributeType(), typeReq.getAttributeTypeGetOwnersExplicitReq().getOnlyKey(), reqID);
                return;
            case REQ_NOT_SET:
            default:
                throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
        }
    }

    private static <T extends Type> T notNull(@Nullable T type) {
        if (type == null) throw TypeDBException.of(MISSING_CONCEPT);
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

    private void setLabel(Type type, String label, UUID reqID) {
        type.setLabel(label);
        transactionSvc.respond(setLabelRes(reqID));
    }

    private void isAbstract(Type type, UUID reqID) {
        transactionSvc.respond(isAbstractRes(reqID, type.isAbstract()));
    }

    private void setAbstract(ThingType thingType, UUID reqID) {
        thingType.setAbstract();
        transactionSvc.respond(setAbstractRes(reqID));
    }

    private void unsetAbstract(ThingType thingType, UUID reqID) {
        thingType.unsetAbstract();
        transactionSvc.respond(unsetAbstractRes(reqID));
    }

    private void getSupertype(Type type, UUID reqID) {
        transactionSvc.respond(getSupertypeRes(reqID, type.getSupertype()));
    }

    private void setSupertype(Type type, ConceptProto.Type supertype, UUID reqID) {
        Type sup = getThingType(supertype);

        if (type.isEntityType()) {
            type.asEntityType().setSupertype(sup.asEntityType());
        } else if (type.isRelationType()) {
            type.asRelationType().setSupertype(sup.asRelationType());
        } else if (type.isAttributeType()) {
            type.asAttributeType().setSupertype(sup.asAttributeType());
        } else {
            throw TypeDBException.of(ILLEGAL_SUPERTYPE_ENCODING, className(type.getClass()));
        }

        transactionSvc.respond(setSupertypeRes(reqID));
    }

    private void getSupertypes(Type type, UUID reqID) {
        transactionSvc.stream(type.getSupertypes(), reqID,
                              types -> getSupertypesResPart(reqID, types));
    }

    private void getSubtypes(Type type, UUID reqID) {
        transactionSvc.stream(type.getSubtypes(), reqID,
                              types -> getSubtypesResPart(reqID, types));
    }

    private void getSubtypesExplicit(Type type, UUID reqID) {
        transactionSvc.stream(type.getSubtypesExplicit(), reqID,
                              types -> getSubtypesExplicitResPart(reqID, types));
    }

    private void getInstances(ThingType thingType, UUID reqID) {
        transactionSvc.stream(thingType.getInstances(), reqID,
                              things -> getInstancesResPart(reqID, things));
    }

    private void getInstancesExplicit(ThingType thingType, UUID reqID) {
        transactionSvc.stream(thingType.getInstancesExplicit(), reqID,
                              things -> getInstancesExplicitResPart(reqID, things));
    }

    private void getOwns(ThingType thingType, ConceptProto.ThingType.GetOwns.Req getOwnsReq, UUID reqID) {
        if (getOwnsReq.getFilterCase() == ConceptProto.ThingType.GetOwns.Req.FilterCase.VALUE_TYPE) {
            getOwnsStream(reqID, thingType.getOwns(
                    valueType(getOwnsReq.getValueType()),
                    getOwnsReq.getKeysOnly()
            ));
        } else getOwnsStream(reqID, thingType.getOwns(getOwnsReq.getKeysOnly()));
    }

    private void getOwnsStream(UUID reqID, SortedIterator.Forwardable<AttributeType, SortedIterator.Order.Asc> atts) {
        transactionSvc.stream(atts, reqID, attributeTypes -> getOwnsResPart(reqID, attributeTypes));
    }

    private void getOwnsExplicit(ThingType thingType, ConceptProto.ThingType.GetOwnsExplicit.Req getOwnsExplicitReq,
                                 UUID reqID) {
        if (getOwnsExplicitReq.getFilterCase() == ConceptProto.ThingType.GetOwnsExplicit.Req.FilterCase.VALUE_TYPE) {
            getOwnsStream(reqID, thingType.getOwnsExplicit(
                    valueType(getOwnsExplicitReq.getValueType()),
                    getOwnsExplicitReq.getKeysOnly()
            ));
        } else getOwnsExplicitStream(reqID, thingType.getOwnsExplicit(getOwnsExplicitReq.getKeysOnly()));
    }

    private void getOwnsExplicitStream(UUID reqID, SortedIterator.Forwardable<AttributeType, SortedIterator.Order.Asc> atts) {
        transactionSvc.stream(atts, reqID, attributeTypes -> getOwnsExplicitResPart(reqID, attributeTypes));
    }

    private void getOwnsOverridden(ThingType thingType, ConceptProto.ThingType.GetOwnsOverridden.Req getOwnsOverriddenReq, UUID reqID) {
        AttributeType attributeType = getThingType(getOwnsOverriddenReq.getAttributeType()).asAttributeType();
        transactionSvc.respond(getOwnsOverriddenRes(reqID, thingType.getOwnsOverridden(attributeType)));
    }

    private void setOwns(ThingType thingType, ConceptProto.ThingType.SetOwns.Req setOwnsRequest, UUID reqID) {
        AttributeType attributeType = getThingType(setOwnsRequest.getAttributeType()).asAttributeType();
        boolean isKey = setOwnsRequest.getIsKey();

        if (setOwnsRequest.hasOverriddenType()) {
            AttributeType overriddenType = getThingType(setOwnsRequest.getOverriddenType()).asAttributeType();
            thingType.setOwns(attributeType, overriddenType, isKey);
        } else {
            thingType.setOwns(attributeType, isKey);
        }
        transactionSvc.respond(setOwnsRes(reqID));
    }

    private void unsetOwns(ThingType thingType, ConceptProto.Type protoAttributeType, UUID reqID) {
        thingType.unsetOwns(getThingType(protoAttributeType).asAttributeType());
        transactionSvc.respond(unsetOwnsRes(reqID));
    }

    private void getPlays(ThingType thingType, UUID reqID) {
        transactionSvc.stream(thingType.getPlays(), reqID, roleTypes -> getPlaysResPart(reqID, roleTypes));
    }

    private void getPlaysExplicit(ThingType thingType, UUID reqID) {
        transactionSvc.stream(thingType.getPlaysExplicit(), reqID, roleTypes -> getPlaysExplicitResPart(reqID, roleTypes));
    }

    private void getPlaysOverridden(ThingType thingType, ConceptProto.ThingType.GetPlaysOverridden.Req getPlaysOverriddenReq, UUID reqID) {
        RoleType roleType = getRoleType(getPlaysOverriddenReq.getRoleType());
        transactionSvc.respond(getPlaysOverriddenRes(reqID, thingType.getPlaysOverridden(roleType)));
    }

    private void setPlays(ThingType thingType, ConceptProto.ThingType.SetPlays.Req setPlaysRequest, UUID reqID) {
        RoleType role = getRoleType(setPlaysRequest.getRole());
        if (setPlaysRequest.hasOverriddenRole()) {
            RoleType overriddenRole = getRoleType(setPlaysRequest.getOverriddenRole());
            thingType.setPlays(role, overriddenRole);
        } else thingType.setPlays(role);
        transactionSvc.respond(setPlaysRes(reqID));
    }

    private void unsetPlays(ThingType thingType, ConceptProto.Type protoRoleType, UUID reqID) {
        thingType.unsetPlays(notNull(getRoleType(protoRoleType)));
        transactionSvc.respond(unsetPlaysRes(reqID));
    }

    private void getOwners(AttributeType attributeType, boolean onlyKey, UUID reqID) {
        transactionSvc.stream(attributeType.getOwners(onlyKey), reqID,
                              owners -> getOwnersResPart(reqID, owners));
    }

    private void getOwnersExplicit(AttributeType attributeType, boolean onlyKey, UUID reqID) {
        transactionSvc.stream(attributeType.getOwnersExplicit(onlyKey), reqID,
                              owners -> getOwnersExplicitResPart(reqID, owners));
    }

    private void put(AttributeType attributeType, ConceptProto.Attribute.Value protoValue, UUID reqID) {
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
                throw TypeDBException.of(MISSING_FIELD, "value");
            default:
                throw TypeDBException.of(BAD_VALUE_TYPE, protoValue.getValueCase());
        }

        transactionSvc.respond(putRes(reqID, attribute));
    }

    private void get(AttributeType attributeType, ConceptProto.Attribute.Value protoValue, UUID reqID) {
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
                // TODO: Unify our exceptions - they should either all be TypeDBException or all be StatusRuntimeException
                throw TypeDBException.of(BAD_VALUE_TYPE);
        }

        transactionSvc.respond(getRes(reqID, attribute));
    }

    private void getRegex(AttributeType attributeType, UUID reqID) {
        transactionSvc.respond(getRegexRes(reqID, attributeType.asString().getRegex()));
    }

    private void setRegex(AttributeType attributeType, String regex, UUID reqID) {
        if (regex.isEmpty()) attributeType.asString().setRegex(null);
        else attributeType.asString().setRegex(Pattern.compile(regex));
        transactionSvc.respond(setRegexRes(reqID));
    }

    private void getRelates(RelationType relationType, UUID reqID) {
        transactionSvc.stream(relationType.getRelates(), reqID,
                roleTypes -> getRelatesResPart(reqID, roleTypes));
    }

    private void getRelatesForRoleLabel(RelationType relationType, String roleLabel, UUID reqID) {
        transactionSvc.respond(getRelatesForRoleLabelRes(reqID, relationType.getRelates(roleLabel)));
    }

    private void setRelates(RelationType relationType, ConceptProto.RelationType.SetRelates.Req setRelatesReq,
                            UUID reqID) {
        if (setRelatesReq.getOverriddenCase() == OVERRIDDEN_LABEL) {
            relationType.setRelates(setRelatesReq.getLabel(), setRelatesReq.getOverriddenLabel());
        } else {
            relationType.setRelates(setRelatesReq.getLabel());
        }
        transactionSvc.respond(setRelatesRes(reqID));
    }

    private void unsetRelates(RelationType relationType, ConceptProto.RelationType.UnsetRelates.Req unsetRelatesReq,
                              UUID reqID) {
        relationType.unsetRelates(unsetRelatesReq.getLabel());
        transactionSvc.respond(unsetRelatesRes(reqID));
    }

    private void getRelationTypes(RoleType roleType, UUID reqID) {
        transactionSvc.stream(roleType.getRelationTypes(), reqID,
                relationTypes -> getRelationTypesResPart(reqID, relationTypes));
    }

    private void getPlayers(RoleType roleType, UUID reqID) {
        transactionSvc.stream(roleType.getPlayers(), reqID,
                players -> getPlayersResPart(reqID, players));
    }

    private void create(EntityType entityType, UUID reqID) {
        transactionSvc.respond(createRes(reqID, entityType.create()));
    }

    private void create(RelationType relationType, UUID reqID) {
        transactionSvc.respond(createRes(reqID, relationType.create()));
    }

    private void delete(Type type, UUID reqID) {
        type.delete();
        transactionSvc.respond(deleteRes(reqID));
    }

}
