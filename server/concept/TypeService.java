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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Concept.Transitivity;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.server.TransactionService;
import com.vaticle.typedb.core.server.common.ResponseBuilder;
import com.vaticle.typedb.protocol.ConceptProto;
import com.vaticle.typedb.protocol.TransactionProto.Transaction;
import com.vaticle.typeql.lang.common.TypeQLToken.Annotation;

import java.time.LocalDateTime;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.MISSING_FIELD;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.server.common.RequestReader.byteStringAsUUID;
import static com.vaticle.typedb.core.server.common.RequestReader.valueType;
import static com.vaticle.typedb.protocol.ConceptProto.RoleType.Req.ReqCase.ROLE_TYPE_DELETE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.RoleType.Req.ReqCase.ROLE_TYPE_GET_PLAYER_INSTANCES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.RoleType.Req.ReqCase.ROLE_TYPE_GET_PLAYER_TYPES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.RoleType.Req.ReqCase.ROLE_TYPE_GET_RELATION_INSTANCES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.RoleType.Req.ReqCase.ROLE_TYPE_GET_RELATION_TYPES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.RoleType.Req.ReqCase.ROLE_TYPE_GET_SUBTYPES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.RoleType.Req.ReqCase.ROLE_TYPE_GET_SUPERTYPES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.RoleType.Req.ReqCase.ROLE_TYPE_GET_SUPERTYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.RoleType.Req.ReqCase.ROLE_TYPE_SET_LABEL_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ATTRIBUTE_TYPE_GET_INSTANCES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ATTRIBUTE_TYPE_GET_OWNERS_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ATTRIBUTE_TYPE_GET_REGEX_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ATTRIBUTE_TYPE_GET_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ATTRIBUTE_TYPE_GET_SUBTYPES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ATTRIBUTE_TYPE_GET_SUPERTYPES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ATTRIBUTE_TYPE_GET_SUPERTYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ATTRIBUTE_TYPE_PUT_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ATTRIBUTE_TYPE_SET_REGEX_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ATTRIBUTE_TYPE_SET_SUPERTYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ENTITY_TYPE_CREATE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ENTITY_TYPE_GET_INSTANCES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ENTITY_TYPE_GET_SUBTYPES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ENTITY_TYPE_GET_SUPERTYPES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ENTITY_TYPE_GET_SUPERTYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.ENTITY_TYPE_SET_SUPERTYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.RELATION_TYPE_CREATE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.RELATION_TYPE_GET_INSTANCES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.RELATION_TYPE_GET_RELATES_FOR_ROLE_LABEL_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.RELATION_TYPE_GET_RELATES_OVERRIDDEN_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.RELATION_TYPE_GET_RELATES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.RELATION_TYPE_GET_SUBTYPES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.RELATION_TYPE_GET_SUPERTYPES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.RELATION_TYPE_GET_SUPERTYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.RELATION_TYPE_SET_RELATES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.RELATION_TYPE_SET_SUPERTYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.RELATION_TYPE_UNSET_RELATES_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_DELETE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_GET_OWNS_OVERRIDDEN_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_GET_OWNS_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_GET_PLAYS_OVERRIDDEN_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_GET_PLAYS_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_GET_SYNTAX_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_SET_ABSTRACT_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_SET_LABEL_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_SET_OWNS_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_SET_PLAYS_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_UNSET_ABSTRACT_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_UNSET_OWNS_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.ThingType.Req.ReqCase.THING_TYPE_UNSET_PLAYS_REQ;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;

public class TypeService {

    private final TransactionService transactionSvc;
    private final ConceptManager conceptMgr;

    private final EnumMap<ConceptProto.RoleType.Req.ReqCase, BiConsumer<ConceptProto.RoleType.Req, UUID>> roleTypeHandlers;
    private final EnumMap<ConceptProto.ThingType.Req.ReqCase, BiConsumer<ConceptProto.ThingType.Req, UUID>> thingTypeHandlers;

    public TypeService(TransactionService transactionSvc, ConceptManager conceptMgr) {
        this.transactionSvc = transactionSvc;
        this.conceptMgr = conceptMgr;

        roleTypeHandlers = new EnumMap<>(ConceptProto.RoleType.Req.ReqCase.class);
        roleTypeHandlers.put(ROLE_TYPE_DELETE_REQ, this::roleTypeDelete);
        roleTypeHandlers.put(ROLE_TYPE_SET_LABEL_REQ, this::roleTypeSetLabel);
        roleTypeHandlers.put(ROLE_TYPE_GET_SUPERTYPE_REQ, this::roleTypeGetSupertype);
        roleTypeHandlers.put(ROLE_TYPE_GET_SUPERTYPES_REQ, this::roleTypeGetSupertypes);
        roleTypeHandlers.put(ROLE_TYPE_GET_SUBTYPES_REQ, this::roleTypeGetSubtypes);
        roleTypeHandlers.put(ROLE_TYPE_GET_RELATION_TYPES_REQ, this::roleTypeGetRelationTypes);
        roleTypeHandlers.put(ROLE_TYPE_GET_PLAYER_TYPES_REQ, this::roleTypeGetPlayerTypes);
        roleTypeHandlers.put(ROLE_TYPE_GET_RELATION_INSTANCES_REQ, this::roleTypeGetRelationInstances);
        roleTypeHandlers.put(ROLE_TYPE_GET_PLAYER_INSTANCES_REQ, this::roleTypeGetPlayerInstances);
        roleTypeHandlers.put(ConceptProto.RoleType.Req.ReqCase.REQ_NOT_SET, this::requestNotSet);
        assert roleTypeHandlers.size() == ConceptProto.RoleType.Req.ReqCase.class.getEnumConstants().length;

        thingTypeHandlers = new EnumMap<>(ConceptProto.ThingType.Req.ReqCase.class);
        thingTypeHandlers.put(THING_TYPE_DELETE_REQ, this::thingTypeDelete);
        thingTypeHandlers.put(THING_TYPE_SET_LABEL_REQ, this::thingTypeSetLabel);
        thingTypeHandlers.put(THING_TYPE_SET_ABSTRACT_REQ, this::thingTypeSetAbstract);
        thingTypeHandlers.put(THING_TYPE_UNSET_ABSTRACT_REQ, this::thingTypeUnsetAbstract);
        thingTypeHandlers.put(THING_TYPE_GET_OWNS_REQ, this::thingTypeGetOwns);
        thingTypeHandlers.put(THING_TYPE_GET_OWNS_OVERRIDDEN_REQ, this::thingTypeGetOwnsOverridden);
        thingTypeHandlers.put(THING_TYPE_SET_OWNS_REQ, this::thingTypeSetOwns);
        thingTypeHandlers.put(THING_TYPE_UNSET_OWNS_REQ, this::thingTypeUnsetOwns);
        thingTypeHandlers.put(THING_TYPE_GET_PLAYS_REQ, this::thingTypeGetPlays);
        thingTypeHandlers.put(THING_TYPE_GET_PLAYS_OVERRIDDEN_REQ, this::thingTypeGetPlaysOverridden);
        thingTypeHandlers.put(THING_TYPE_SET_PLAYS_REQ, this::thingTypeSetPlays);
        thingTypeHandlers.put(THING_TYPE_UNSET_PLAYS_REQ, this::thingTypeUnsetPlays);
        thingTypeHandlers.put(THING_TYPE_GET_SYNTAX_REQ, this::thingTypeGetSyntax);
        thingTypeHandlers.put(ENTITY_TYPE_CREATE_REQ, this::entityTypeCreate);
        thingTypeHandlers.put(ENTITY_TYPE_GET_SUPERTYPE_REQ, this::entityTypeGetSupertype);
        thingTypeHandlers.put(ENTITY_TYPE_SET_SUPERTYPE_REQ, this::entityTypeSetSupertype);
        thingTypeHandlers.put(ENTITY_TYPE_GET_SUPERTYPES_REQ, this::entityTypeGetSupertypes);
        thingTypeHandlers.put(ENTITY_TYPE_GET_SUBTYPES_REQ, this::entityTypeGetSubtypes);
        thingTypeHandlers.put(ENTITY_TYPE_GET_INSTANCES_REQ, this::entityTypeGetInstances);
        thingTypeHandlers.put(RELATION_TYPE_CREATE_REQ, this::relationTypeCreate);
        thingTypeHandlers.put(RELATION_TYPE_GET_SUPERTYPE_REQ, this::relationTypeGetSupertype);
        thingTypeHandlers.put(RELATION_TYPE_SET_SUPERTYPE_REQ, this::relationTypeSetSupertype);
        thingTypeHandlers.put(RELATION_TYPE_GET_SUPERTYPES_REQ, this::relationTypeGetSupertypes);
        thingTypeHandlers.put(RELATION_TYPE_GET_SUBTYPES_REQ, this::relationTypeGetSubtypes);
        thingTypeHandlers.put(RELATION_TYPE_GET_INSTANCES_REQ, this::relationTypeGetInstances);
        thingTypeHandlers.put(RELATION_TYPE_GET_RELATES_REQ, this::relationTypeGetRelates);
        thingTypeHandlers.put(RELATION_TYPE_GET_RELATES_FOR_ROLE_LABEL_REQ, this::relationTypeGetRelatesForRoleLabel);
        thingTypeHandlers.put(RELATION_TYPE_GET_RELATES_OVERRIDDEN_REQ, this::relationTypeGetRelatesOverridden);
        thingTypeHandlers.put(RELATION_TYPE_SET_RELATES_REQ, this::relationTypeSetRelates);
        thingTypeHandlers.put(RELATION_TYPE_UNSET_RELATES_REQ, this::relationTypeUnsetRelates);
        thingTypeHandlers.put(ATTRIBUTE_TYPE_PUT_REQ, this::attributeTypePut);
        thingTypeHandlers.put(ATTRIBUTE_TYPE_GET_REQ, this::attributeTypeGet);
        thingTypeHandlers.put(ATTRIBUTE_TYPE_GET_SUPERTYPE_REQ, this::attributeTypeGetSupertype);
        thingTypeHandlers.put(ATTRIBUTE_TYPE_SET_SUPERTYPE_REQ, this::attributeTypeSetSupertype);
        thingTypeHandlers.put(ATTRIBUTE_TYPE_GET_SUPERTYPES_REQ, this::attributeTypeGetSupertypes);
        thingTypeHandlers.put(ATTRIBUTE_TYPE_GET_SUBTYPES_REQ, this::attributeTypeGetSubtypes);
        thingTypeHandlers.put(ATTRIBUTE_TYPE_GET_INSTANCES_REQ, this::attributeTypeGetInstances);
        thingTypeHandlers.put(ATTRIBUTE_TYPE_GET_REGEX_REQ, this::attributeTypeGetRegex);
        thingTypeHandlers.put(ATTRIBUTE_TYPE_SET_REGEX_REQ, this::attributeTypeSetRegex);
        thingTypeHandlers.put(ATTRIBUTE_TYPE_GET_OWNERS_REQ, this::attributeTypeGetOwners);
        thingTypeHandlers.put(ConceptProto.ThingType.Req.ReqCase.REQ_NOT_SET, this::requestNotSet);
        assert thingTypeHandlers.size() == ConceptProto.ThingType.Req.ReqCase.class.getEnumConstants().length;
    }

    public void execute(Transaction.Req req) {
        ConceptProto.Type.Req typeReq = req.getTypeReq();
        UUID reqID = byteStringAsUUID(req.getReqId());
        switch (typeReq.getReqCase()) {
            case THING_TYPE_REQ:
                ConceptProto.ThingType.Req thingTypeReq = typeReq.getThingTypeReq();
                if (!thingTypeHandlers.containsKey(thingTypeReq.getReqCase())) throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
                thingTypeHandlers.get(thingTypeReq.getReqCase()).accept(thingTypeReq, reqID);
                return;
            case ROLE_TYPE_REQ:
                ConceptProto.RoleType.Req roleTypeReq = typeReq.getRoleTypeReq();
                if (!roleTypeHandlers.containsKey(roleTypeReq.getReqCase())) throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
                roleTypeHandlers.get(roleTypeReq.getReqCase()).accept(roleTypeReq, reqID);
                return;
            case REQ_NOT_SET:
            default:
                throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
        }
    }

    private void roleTypeDelete(ConceptProto.RoleType.Req roleTypeReq, UUID reqID) {
        getRoleType(roleTypeReq).delete();
        transactionSvc.respond(ResponseBuilder.Type.RoleType.deleteRes(reqID));
    }

    private void roleTypeSetLabel(ConceptProto.RoleType.Req roleTypeReq, UUID reqID) {
        getRoleType(roleTypeReq).setLabel(roleTypeReq.getRoleTypeSetLabelReq().getLabel());
        transactionSvc.respond(ResponseBuilder.Type.RoleType.setLabelRes(reqID));
    }

    private void roleTypeGetSupertype(ConceptProto.RoleType.Req roleTypeReq, UUID reqID) {
        transactionSvc.respond(ResponseBuilder.Type.RoleType.getSupertypeRes(reqID, getRoleType(roleTypeReq).getSupertype()));
    }

    private void roleTypeGetSupertypes(ConceptProto.RoleType.Req roleTypeReq, UUID reqID) {
        transactionSvc.stream(
                getRoleType(roleTypeReq).getSupertypes(), reqID,
                types -> ResponseBuilder.Type.RoleType.getSupertypesResPart(reqID, types)
        );
    }

    private void roleTypeGetSubtypes(ConceptProto.RoleType.Req roleTypeReq, UUID reqID) {
        transactionSvc.stream(
                getRoleType(roleTypeReq).getSubtypes(), reqID,
                types -> ResponseBuilder.Type.RoleType.getSubtypesResPart(reqID, types)
        );
    }

    private void roleTypeGetRelationTypes(ConceptProto.RoleType.Req roleTypeReq, UUID reqID) {
        transactionSvc.stream(
                getRoleType(roleTypeReq).getRelationTypes(), reqID,
                relationTypes -> ResponseBuilder.Type.RoleType.getRelationTypesResPart(reqID, relationTypes)
        );
    }

    private void roleTypeGetPlayerTypes(ConceptProto.RoleType.Req roleTypeReq, UUID reqID) {
        ConceptProto.RoleType.GetPlayerTypes.Req req = roleTypeReq.getRoleTypeGetPlayerTypesReq();
        transactionSvc.stream(
                getRoleType(roleTypeReq).getPlayerTypes(getTransitivity(req.getTransitivity())), reqID,
                types -> ResponseBuilder.Type.RoleType.getPlayerTypesResPart(reqID, types)
        );
    }

    private void roleTypeGetRelationInstances(ConceptProto.RoleType.Req roleTypeReq, UUID reqID) {
        ConceptProto.RoleType.GetRelationInstances.Req req = roleTypeReq.getRoleTypeGetRelationInstancesReq();
        transactionSvc.stream(
                getRoleType(roleTypeReq).getRelationInstances(getTransitivity(req.getTransitivity())), reqID,
                relations -> ResponseBuilder.Type.RoleType.getRelationInstancesResPart(reqID, relations)
        );
    }

    private void roleTypeGetPlayerInstances(ConceptProto.RoleType.Req roleTypeReq, UUID reqID) {
        ConceptProto.RoleType.GetPlayerInstances.Req req = roleTypeReq.getRoleTypeGetPlayerInstancesReq();
        transactionSvc.stream(
                getRoleType(roleTypeReq).getPlayerInstances(getTransitivity(req.getTransitivity())), reqID,
                players -> ResponseBuilder.Type.RoleType.getPlayerInstancesResPart(reqID, players)
        );
    }

    private void requestNotSet(ConceptProto.RoleType.Req thingReq, UUID reqID) {
        throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
    }

    private void thingTypeDelete(ConceptProto.ThingType.Req typeReq, UUID reqID) {
        getThingType(typeReq).delete();
        transactionSvc.respond(ResponseBuilder.Type.ThingType.deleteRes(reqID));
    }

    private void thingTypeSetLabel(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        getThingType(thingTypeReq).setLabel(thingTypeReq.getThingTypeSetLabelReq().getLabel());
        transactionSvc.respond(ResponseBuilder.Type.ThingType.setLabelRes(reqID));
    }

    private void thingTypeSetAbstract(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        getThingType(thingTypeReq).setAbstract();
        transactionSvc.respond(ResponseBuilder.Type.ThingType.setAbstractRes(reqID));
    }

    private void thingTypeUnsetAbstract(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        getThingType(thingTypeReq).unsetAbstract();
        transactionSvc.respond(ResponseBuilder.Type.ThingType.unsetAbstractRes(reqID));
    }

    private void thingTypeGetOwns(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        ConceptProto.ThingType.GetOwns.Req getOwnsReq = thingTypeReq.getThingTypeGetOwnsReq();
        FunctionalIterator<AttributeType> attributes;
        Set<Annotation> annotations = getAnnotations(getOwnsReq.getAnnotationsList());
        Transitivity transitivity = getTransitivity(getOwnsReq.getTransitivity());
        if (getOwnsReq.hasValueType()) {
            attributes = getThingType(thingTypeReq).getOwns(transitivity, valueType(getOwnsReq.getValueType()), annotations)
                    .map(ThingType.Owns::attributeType);
        } else attributes = getThingType(thingTypeReq).getOwns(transitivity, annotations).map(ThingType.Owns::attributeType);
        transactionSvc.stream(attributes, reqID, attributeTypes -> ResponseBuilder.Type.ThingType.getOwnsResPart(reqID, attributeTypes));
    }

    private void thingTypeGetOwnsOverridden(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        ConceptProto.ThingType.GetOwnsOverridden.Req getOwnsOverriddenReq = thingTypeReq.getThingTypeGetOwnsOverriddenReq();
        AttributeType attributeType = getAttributeType(getOwnsOverriddenReq.getAttributeType());
        transactionSvc.respond(ResponseBuilder.Type.ThingType.getOwnsOverriddenRes(reqID, getThingType(thingTypeReq).getOwnsOverridden(attributeType)));
    }

    private void thingTypeSetOwns(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        ConceptProto.ThingType.SetOwns.Req setOwnsReq = thingTypeReq.getThingTypeSetOwnsReq();
        AttributeType attributeType = getAttributeType(setOwnsReq.getAttributeType());
        Set<Annotation> annotations = getAnnotations(setOwnsReq.getAnnotationsList());

        if (setOwnsReq.hasOverriddenType()) {
            AttributeType overriddenType = getAttributeType(setOwnsReq.getOverriddenType());
            getThingType(thingTypeReq).setOwns(attributeType, overriddenType, annotations);
        } else getThingType(thingTypeReq).setOwns(attributeType, annotations);
        transactionSvc.respond(ResponseBuilder.Type.ThingType.setOwnsRes(reqID));
    }

    private void thingTypeUnsetOwns(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        ConceptProto.ThingType.UnsetOwns.Req unsetOwnsReq = thingTypeReq.getThingTypeUnsetOwnsReq();
        getThingType(thingTypeReq).unsetOwns(getAttributeType(unsetOwnsReq.getAttributeType()));
        transactionSvc.respond(ResponseBuilder.Type.ThingType.unsetOwnsRes(reqID));
    }

    private void thingTypeGetPlays(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        ConceptProto.ThingType.GetPlays.Req getPlaysReq = thingTypeReq.getThingTypeGetPlaysReq();
        transactionSvc.stream(
                getThingType(thingTypeReq).getPlays(getTransitivity(getPlaysReq.getTransitivity())), reqID,
                roleTypes -> ResponseBuilder.Type.ThingType.getPlaysResPart(reqID, roleTypes)
        );
    }

    private void thingTypeGetPlaysOverridden(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        ConceptProto.ThingType.GetPlaysOverridden.Req getPlaysOverriddenReq = thingTypeReq.getThingTypeGetPlaysOverriddenReq();
        RoleType roleType = getRoleType(getPlaysOverriddenReq.getRoleType());
        transactionSvc.respond(ResponseBuilder.Type.ThingType.getPlaysOverriddenRes(reqID, getThingType(thingTypeReq).getPlaysOverridden(roleType)));
    }

    private void thingTypeSetPlays(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        ConceptProto.ThingType.SetPlays.Req setPlaysRequest = thingTypeReq.getThingTypeSetPlaysReq();
        RoleType role = getRoleType(setPlaysRequest.getRoleType());
        if (setPlaysRequest.hasOverriddenRoleType()) {
            getThingType(thingTypeReq).setPlays(role, getRoleType(setPlaysRequest.getOverriddenRoleType()));
        } else getThingType(thingTypeReq).setPlays(role);
        transactionSvc.respond(ResponseBuilder.Type.ThingType.setPlaysRes(reqID));
    }

    private void thingTypeUnsetPlays(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        ConceptProto.ThingType.UnsetPlays.Req unsetPlaysReq = thingTypeReq.getThingTypeUnsetPlaysReq();
        getThingType(thingTypeReq).unsetPlays(notNull(getRoleType(unsetPlaysReq.getRoleType())));
        transactionSvc.respond(ResponseBuilder.Type.ThingType.unsetPlaysRes(reqID));
    }

    private void thingTypeGetSyntax(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        transactionSvc.respond(ResponseBuilder.Type.ThingType.getSyntaxRes(reqID, getThingType(thingTypeReq).getSyntax()));
    }

    private void entityTypeCreate(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        transactionSvc.respond(ResponseBuilder.Type.EntityType.createRes(reqID, getEntityType(thingTypeReq).create()));
    }

    private void entityTypeGetSupertype(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        transactionSvc.respond(ResponseBuilder.Type.EntityType.getSupertypeRes(reqID, getEntityType(thingTypeReq).getSupertype()));
    }

    private void entityTypeSetSupertype(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        EntityType supertype = getEntityType(thingTypeReq.getEntityTypeSetSupertypeReq().getEntityType());
        getEntityType(thingTypeReq).setSupertype(supertype);
        transactionSvc.respond(ResponseBuilder.Type.EntityType.setSupertypeRes(reqID));
    }

    private void entityTypeGetSupertypes(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        transactionSvc.stream(
                getEntityType(thingTypeReq).getSupertypes(), reqID,
                types -> ResponseBuilder.Type.EntityType.getSupertypesResPart(reqID, types)
        );
    }

    private void entityTypeGetSubtypes(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        Transitivity transitivity = getTransitivity(thingTypeReq.getEntityTypeGetSubtypesReq().getTransitivity());
        transactionSvc.stream(
                getEntityType(thingTypeReq).getSubtypes(transitivity), reqID,
                types -> ResponseBuilder.Type.EntityType.getSubtypesResPart(reqID, types)
        );
    }

    private void entityTypeGetInstances(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        Transitivity transitivity = getTransitivity(thingTypeReq.getEntityTypeGetInstancesReq().getTransitivity());
        transactionSvc.stream(
                getEntityType(thingTypeReq).getInstances(transitivity), reqID,
                things -> ResponseBuilder.Type.EntityType.getInstancesResPart(reqID, things)
        );
    }

    private void relationTypeCreate(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        transactionSvc.respond(ResponseBuilder.Type.RelationType.createRes(reqID, getRelationType(thingTypeReq).create()));
    }

    private void relationTypeGetSupertype(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        transactionSvc.respond(ResponseBuilder.Type.RelationType.getSupertypeRes(reqID, getRelationType(thingTypeReq).getSupertype()));
    }

    private void relationTypeSetSupertype(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        RelationType supertype = getRelationType(thingTypeReq.getRelationTypeSetSupertypeReq().getRelationType());
        getThingType(thingTypeReq).asRelationType().setSupertype(supertype);
        transactionSvc.respond(ResponseBuilder.Type.RelationType.setSupertypeRes(reqID));
    }

    private void relationTypeGetSupertypes(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        transactionSvc.stream(
                getRelationType(thingTypeReq).getSupertypes(), reqID,
                types -> ResponseBuilder.Type.RelationType.getSupertypesResPart(reqID, types)
        );
    }

    private void relationTypeGetSubtypes(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        Transitivity transitivity = getTransitivity(thingTypeReq.getRelationTypeGetSubtypesReq().getTransitivity());
        transactionSvc.stream(
                getRelationType(thingTypeReq).getSubtypes(transitivity), reqID,
                types -> ResponseBuilder.Type.RelationType.getSubtypesResPart(reqID, types)
        );
    }

    private void relationTypeGetInstances(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        Transitivity transitivity = getTransitivity(thingTypeReq.getRelationTypeGetInstancesReq().getTransitivity());
        transactionSvc.stream(
                getRelationType(thingTypeReq).getInstances(transitivity), reqID,
                things -> ResponseBuilder.Type.RelationType.getInstancesResPart(reqID, things)
        );
    }

    private void relationTypeGetRelates(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        Transitivity transitivity = getTransitivity(thingTypeReq.getRelationTypeGetRelatesReq().getTransitivity());
        transactionSvc.stream(
                getRelationType(thingTypeReq).getRelates(transitivity), reqID,
                roleTypes -> ResponseBuilder.Type.RelationType.getRelatesResPart(reqID, roleTypes)
        );
    }

    private void relationTypeGetRelatesForRoleLabel(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        String roleLabel = thingTypeReq.getRelationTypeGetRelatesForRoleLabelReq().getLabel();
        transactionSvc.respond(ResponseBuilder.Type.RelationType.getRelatesForRoleLabelRes(reqID, getRelationType(thingTypeReq).getRelates(roleLabel)));
    }

    private void relationTypeGetRelatesOverridden(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        String roleLabel = thingTypeReq.getRelationTypeGetRelatesOverriddenReq().getLabel();
        transactionSvc.respond(ResponseBuilder.Type.RelationType.getRelatesOverriddenRes(reqID, getRelationType(thingTypeReq).getRelatesOverridden(roleLabel)));
    }

    private void relationTypeSetRelates(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        ConceptProto.RelationType.SetRelates.Req setRelatesReq = thingTypeReq.getRelationTypeSetRelatesReq();
        if (setRelatesReq.hasOverriddenLabel()) {
            getRelationType(thingTypeReq).setRelates(setRelatesReq.getLabel(), setRelatesReq.getOverriddenLabel());
        } else {
            getRelationType(thingTypeReq).setRelates(setRelatesReq.getLabel());
        }
        transactionSvc.respond(ResponseBuilder.Type.RelationType.setRelatesRes(reqID));
    }

    private void relationTypeUnsetRelates(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        String roleLabel = thingTypeReq.getRelationTypeUnsetRelatesReq().getLabel();
        getRelationType(thingTypeReq).unsetRelates(roleLabel);
        transactionSvc.respond(ResponseBuilder.Type.RelationType.unsetRelatesRes(reqID));
    }


    private void attributeTypePut(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        AttributeType attributeType = getAttributeType(thingTypeReq);
        ConceptProto.Value protoValue = thingTypeReq.getAttributeTypePutReq().getValue();

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

        transactionSvc.respond(ResponseBuilder.Type.AttributeType.putRes(reqID, attribute));
    }

    private void attributeTypeGet(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        AttributeType attributeType = getAttributeType(thingTypeReq);
        ConceptProto.Value protoValue = thingTypeReq.getAttributeTypeGetReq().getValue();

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
                throw TypeDBException.of(BAD_VALUE_TYPE);
        }

        transactionSvc.respond(ResponseBuilder.Type.AttributeType.getRes(reqID, attribute));
    }

    private void attributeTypeGetSupertype(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        transactionSvc.respond(ResponseBuilder.Type.AttributeType.getSupertypeRes(reqID, getAttributeType(thingTypeReq).getSupertype()));
    }

    private void attributeTypeSetSupertype(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        AttributeType supertype = getAttributeType(thingTypeReq.getAttributeTypeSetSupertypeReq().getAttributeType());
        getThingType(thingTypeReq).asAttributeType().setSupertype(supertype);
        transactionSvc.respond(ResponseBuilder.Type.AttributeType.setSupertypeRes(reqID));
    }

    private void attributeTypeGetSupertypes(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        transactionSvc.stream(
                getAttributeType(thingTypeReq).getSupertypes(), reqID,
                types -> ResponseBuilder.Type.AttributeType.getSupertypesResPart(reqID, types)
        );
    }

    private void attributeTypeGetSubtypes(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        AttributeType attributeType = getAttributeType(thingTypeReq);
        switch (thingTypeReq.getAttributeTypeGetSubtypesReq().getValueType()) {
            case BOOLEAN: attributeType = attributeType.asBoolean(); break;
            case DATETIME: attributeType = attributeType.asDateTime(); break;
            case DOUBLE: attributeType = attributeType.asDouble(); break;
            case LONG: attributeType = attributeType.asLong(); break;
            case STRING: attributeType = attributeType.asString(); break;
            case OBJECT: default: break;
        }
        Transitivity transitivity = getTransitivity(thingTypeReq.getAttributeTypeGetSubtypesReq().getTransitivity());
        transactionSvc.stream(
                attributeType.getSubtypes(transitivity), reqID,
                types -> ResponseBuilder.Type.AttributeType.getSubtypesResPart(reqID, types)
        );
    }

    private void attributeTypeGetInstances(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        Transitivity transitivity = getTransitivity(thingTypeReq.getAttributeTypeGetInstancesReq().getTransitivity());
        transactionSvc.stream(
                getAttributeType(thingTypeReq).getInstances(transitivity), reqID,
                things -> ResponseBuilder.Type.AttributeType.getInstancesResPart(reqID, things)
        );
    }

    private void attributeTypeGetRegex(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        transactionSvc.respond(ResponseBuilder.Type.AttributeType.getRegexRes(reqID, getAttributeType(thingTypeReq).asString().getRegex()));
    }

    private void attributeTypeSetRegex(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        AttributeType.String attributeType = getAttributeType(thingTypeReq).asString();
        String regex = thingTypeReq.getAttributeTypeSetRegexReq().getRegex();
        if (regex.isEmpty()) attributeType.setRegex(null);
        else attributeType.setRegex(Pattern.compile(regex));
        transactionSvc.respond(ResponseBuilder.Type.AttributeType.setRegexRes(reqID));
    }

    private void attributeTypeGetOwners(ConceptProto.ThingType.Req thingTypeReq, UUID reqID) {
        ConceptProto.AttributeType.GetOwners.Req getOwnersReq = thingTypeReq.getAttributeTypeGetOwnersReq();
        Set<Annotation> annotations = getAnnotations(getOwnersReq.getAnnotationsList());
        Transitivity transitivity = getTransitivity(getOwnersReq.getTransitivity());
        transactionSvc.stream(
                getAttributeType(thingTypeReq).getOwners(transitivity, annotations), reqID,
                owners -> ResponseBuilder.Type.AttributeType.getOwnersResPart(reqID, owners)
        );
    }

    private void requestNotSet(ConceptProto.ThingType.Req thingReq, UUID reqID) {
        throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
    }

    private static <T extends Type> T notNull(@Nullable T type) {
        if (type == null) throw TypeDBException.of(MISSING_CONCEPT);
        return type;
    }

    private ThingType getThingType(ConceptProto.ThingType.Req typeReq) {
        return notNull(conceptMgr.getThingType(typeReq.getLabel()));
    }

    private EntityType getEntityType(ConceptProto.ThingType.Req typeReq) {
        return conceptMgr.getEntityType(typeReq.getLabel());
    }

    private EntityType getEntityType(ConceptProto.EntityType protoType) {
        return conceptMgr.getEntityType(protoType.getLabel());
    }

    private RelationType getRelationType(ConceptProto.ThingType.Req typeReq) {
        return conceptMgr.getRelationType(typeReq.getLabel());
    }

    private RelationType getRelationType(ConceptProto.RelationType protoType) {
        return conceptMgr.getRelationType(protoType.getLabel());
    }

    private AttributeType getAttributeType(ConceptProto.ThingType.Req typeReq) {
        return conceptMgr.getAttributeType(typeReq.getLabel());
    }

    private AttributeType getAttributeType(ConceptProto.AttributeType protoType) {
        return conceptMgr.getAttributeType(protoType.getLabel());
    }

    private RoleType getRoleType(ConceptProto.RoleType.Req typeReq) {
        return notNull(conceptMgr.getRelationType(typeReq.getScope())).getRelates(typeReq.getLabel());
    }

    private RoleType getRoleType(ConceptProto.RoleType protoRoleType) {
        RelationType relationType = conceptMgr.getRelationType(protoRoleType.getScope());
        if (relationType != null) return relationType.getRelates(protoRoleType.getLabel());
        else return null;
    }

    public static Transitivity getTransitivity(ConceptProto.Type.Transitivity proto) {
        switch (proto) {
            case EXPLICIT: return Transitivity.EXPLICIT;
            case TRANSITIVE: return Transitivity.TRANSITIVE;
            case UNRECOGNIZED:
            default:
                throw TypeDBException.of(BAD_VALUE_TYPE, proto);
        }
    }

    private Set<Annotation> getAnnotations(List<ConceptProto.Type.Annotation> protoAnnotations) {
        return iterate(protoAnnotations).map(
                annotation -> {
                    switch (annotation.getAnnotationCase()) {
                        case KEY: return Annotation.KEY;
                        case UNIQUE: return Annotation.UNIQUE;
                        case ANNOTATION_NOT_SET:
                        default: throw TypeDBException.of(ILLEGAL_ARGUMENT);
                    }
                }
        ).toSet();
    }
}
