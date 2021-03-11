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
import grakn.core.concept.thing.Entity;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.concept.type.Type;
import grakn.core.server.TransactionService;
import grakn.core.server.common.ResponseBuilder;
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
import static grakn.core.server.common.ResponseBuilder.Concept.thing;
import static grakn.core.server.common.ResponseBuilder.Concept.type;
import static grakn.core.server.common.ResponseBuilder.Concept.valueType;
import static grakn.protocol.ConceptProto.RelationType.SetRelates.Req.OverriddenCase.OVERRIDDEN_LABEL;
import static grakn.protocol.ConceptProto.ThingType.GetOwns.Req.FilterCase.VALUE_TYPE;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.toList;

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
            case ROLE_TYPE_GET_RELATION_TYPE_REQ:
                getRelationType(type.asRoleType(), request);
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

    private static Transaction.Res response(Transaction.Req request, ConceptProto.Type.Res.Builder response) {
        return Transaction.Res.newBuilder().setId(request.getId()).setTypeRes(response).build();
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

    private void delete(Type type, Transaction.Req request) {
        type.delete();
        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setTypeDeleteRes(
                ConceptProto.Type.Delete.Res.getDefaultInstance()
        )));
    }

    private void setLabel(Type type, String label, Transaction.Req request) {
        type.setLabel(label);
        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setTypeSetLabelRes(
                ConceptProto.Type.SetLabel.Res.getDefaultInstance()
        )));
    }

    private void isAbstract(Type type, Transaction.Req request) {
        ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder()
                .setTypeIsAbstractRes(ConceptProto.Type.IsAbstract.Res.newBuilder().setAbstract(type.isAbstract()));
        transactionSrv.respond(response(request, response));
    }

    private void getSupertype(Type type, Transaction.Req request) {
        ConceptProto.Type.GetSupertype.Res.Builder getSupertypeRes = ConceptProto.Type.GetSupertype.Res.newBuilder();
        Type superType = type.getSupertype();
        if (superType != null) getSupertypeRes.setType(type(superType));
        ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder()
                .setTypeGetSupertypeRes(getSupertypeRes);
        transactionSrv.respond(response(request, response));
    }

    private void setSupertype(Type type, ConceptProto.Type supertype, Transaction.Req request) {
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

        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setTypeSetSupertypeRes(
                ConceptProto.Type.SetSupertype.Res.getDefaultInstance()
        )));
    }

    private void getSupertypes(Type type, Transaction.Req request) {
        transactionSrv.stream(type.getSupertypes().iterator(), request.getId(), cons -> response(
                request, ConceptProto.Type.Res.newBuilder().setTypeGetSupertypesRes(
                        ConceptProto.Type.GetSupertypes.Res.newBuilder().addAllTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(toList())))
        ));
    }

    private void getSubtypes(Type type, Transaction.Req request) {
        transactionSrv.stream(type.getSubtypes().iterator(), request.getId(), cons -> response(
                request, ConceptProto.Type.Res.newBuilder().setTypeGetSubtypesRes(
                        ConceptProto.Type.GetSubtypes.Res.newBuilder().addAllTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(toList())))
        ));
    }

    private void getInstances(ThingType thingType, Transaction.Req request) {
        transactionSrv.stream(thingType.getInstances().iterator(), request.getId(), cons -> response(
                request, ConceptProto.Type.Res.newBuilder().setThingTypeGetInstancesRes(
                        ConceptProto.ThingType.GetInstances.Res.newBuilder().addAllThings(
                                cons.stream().map(ResponseBuilder.Concept::thing).collect(toList())))
        ));
    }

    private void setAbstract(ThingType thingType, Transaction.Req request) {
        thingType.setAbstract();
        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setThingTypeSetAbstractRes(
                ConceptProto.ThingType.SetAbstract.Res.getDefaultInstance()
        )));
    }

    private void unsetAbstract(ThingType thingType, Transaction.Req request) {
        thingType.unsetAbstract();
        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setThingTypeUnsetAbstractRes(
                ConceptProto.ThingType.UnsetAbstract.Res.getDefaultInstance()
        )));
    }

    private void getOwns(ThingType thingType, ConceptProto.Type.Req typeReq, Transaction.Req request) {
        if (typeReq.getThingTypeGetOwnsReq().getFilterCase() == VALUE_TYPE) {
            getOwns(thingType, valueType(typeReq.getThingTypeGetOwnsReq().getValueType()),
                    typeReq.getThingTypeGetOwnsReq().getKeysOnly(), request);
        } else {
            getOwns(thingType, typeReq.getThingTypeGetOwnsReq().getKeysOnly(), request);
        }
    }

    private void getOwns(ThingType thingType, boolean keysOnly, Transaction.Req request) {
        transactionSrv.stream(thingType.getOwns(keysOnly).iterator(), request.getId(), cons -> response(
                request, ConceptProto.Type.Res.newBuilder().setThingTypeGetOwnsRes(
                        ConceptProto.ThingType.GetOwns.Res.newBuilder().addAllAttributeTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(toList())))
        ));
    }

    private void getOwns(ThingType thingType, AttributeType.ValueType valueType,
                         boolean keysOnly, Transaction.Req request) {
        transactionSrv.stream(thingType.getOwns(valueType, keysOnly).iterator(), request.getId(), cons -> response(
                request, ConceptProto.Type.Res.newBuilder().setThingTypeGetOwnsRes(
                        ConceptProto.ThingType.GetOwns.Res.newBuilder().addAllAttributeTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(toList())))
        ));
    }

    private void getPlays(ThingType thingType, Transaction.Req request) {
        transactionSrv.stream(thingType.getPlays().iterator(), request.getId(), cons -> response(
                request, ConceptProto.Type.Res.newBuilder().setThingTypeGetPlaysRes(
                        ConceptProto.ThingType.GetPlays.Res.newBuilder().addAllRoles(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(toList())))
        ));
    }

    private void setOwns(ThingType thingType, ConceptProto.ThingType.SetOwns.Req setOwnsRequest,
                         Transaction.Req request) {
        AttributeType attributeType = getThingType(setOwnsRequest.getAttributeType()).asAttributeType();
        boolean isKey = setOwnsRequest.getIsKey();

        if (setOwnsRequest.hasOverriddenType()) {
            AttributeType overriddenType = getThingType(setOwnsRequest.getOverriddenType()).asAttributeType();
            thingType.setOwns(attributeType, overriddenType, isKey);
        } else {
            thingType.setOwns(attributeType, isKey);
        }
        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setThingTypeSetOwnsRes(
                ConceptProto.ThingType.SetOwns.Res.getDefaultInstance()
        )));
    }

    private void setPlays(ThingType thingType, ConceptProto.ThingType.SetPlays.Req setPlaysRequest,
                          Transaction.Req request) {
        RoleType role = getRoleType(setPlaysRequest.getRole());
        if (setPlaysRequest.hasOverriddenRole()) {
            RoleType overriddenRole = getRoleType(setPlaysRequest.getOverriddenRole());
            thingType.setPlays(role, overriddenRole);
        } else {
            thingType.setPlays(role);
        }
        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setThingTypeSetPlaysRes(
                ConceptProto.ThingType.SetPlays.Res.getDefaultInstance()
        )));
    }

    private void unsetOwns(ThingType thingType, ConceptProto.Type protoAttributeType, Transaction.Req request) {
        AttributeType attributeType = getThingType(protoAttributeType).asAttributeType();
        thingType.unsetOwns(attributeType);
        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setThingTypeUnsetOwnsRes(
                ConceptProto.ThingType.UnsetOwns.Res.getDefaultInstance()
        )));
    }

    private void unsetPlays(ThingType thingType, ConceptProto.Type protoRoleType, Transaction.Req request) {
        RoleType role = notNull(getRoleType(protoRoleType));
        thingType.unsetPlays(role);
        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setThingTypeUnsetPlaysRes(
                ConceptProto.ThingType.UnsetPlays.Res.getDefaultInstance()
        )));
    }

    private void create(EntityType entityType, Transaction.Req request) {
        Entity entity = entityType.create();
        ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder()
                .setEntityTypeCreateRes(ConceptProto.EntityType.Create.Res.newBuilder().setEntity(thing(entity)));
        transactionSrv.respond(response(request, response));
    }

    private void getOwners(AttributeType attributeType, boolean onlyKey, Transaction.Req request) {
        transactionSrv.stream(attributeType.getOwners(onlyKey).iterator(), request.getId(), cons -> response(
                request, ConceptProto.Type.Res.newBuilder().setAttributeTypeGetOwnersRes(
                        ConceptProto.AttributeType.GetOwners.Res.newBuilder().addAllOwners(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(toList())))
        ));
    }

    private void put(AttributeType attributeType, ConceptProto.Attribute.Value protoValue, Transaction.Req request) {
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

        ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder().setAttributeTypePutRes(
                ConceptProto.AttributeType.Put.Res.newBuilder().setAttribute(thing(attribute))
        );
        transactionSrv.respond(response(request, response));
    }

    private void get(AttributeType attributeType, ConceptProto.Attribute.Value protoValue, Transaction.Req request) {
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

        ConceptProto.AttributeType.Get.Res.Builder getAttributeTypeRes = ConceptProto.AttributeType.Get.Res.newBuilder();
        if (attribute != null) getAttributeTypeRes.setAttribute(thing(attribute));
        ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder().setAttributeTypeGetRes(getAttributeTypeRes);
        transactionSrv.respond(response(request, response));
    }

    private void getRegex(AttributeType attributeType, Transaction.Req request) {
        Pattern regex = attributeType.asString().getRegex();
        ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder().setAttributeTypeGetRegexRes(
                ConceptProto.AttributeType.GetRegex.Res.newBuilder().setRegex((regex != null) ? regex.pattern() : "")
        );
        transactionSrv.respond(response(request, response));
    }

    private void setRegex(AttributeType attributeType, String regex, Transaction.Req request) {
        if (regex.isEmpty()) attributeType.asString().setRegex(null);
        else attributeType.asString().setRegex(Pattern.compile(regex));
        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setAttributeTypeSetRegexRes(
                ConceptProto.AttributeType.SetRegex.Res.getDefaultInstance())
        ));
    }

    private void create(RelationType relationType, Transaction.Req request) {
        Relation relation = relationType.create();
        ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder().setRelationTypeCreateRes(
                ConceptProto.RelationType.Create.Res.newBuilder().setRelation(thing(relation))
        );
        transactionSrv.respond(response(request, response));
    }

    private void getRelates(RelationType relationType, Transaction.Req request) {
        transactionSrv.stream(relationType.getRelates().iterator(), request.getId(), cons -> response(
                request, ConceptProto.Type.Res.newBuilder().setRelationTypeGetRelatesRes(
                        ConceptProto.RelationType.GetRelates.Res.newBuilder().addAllRoles(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(toList())))
        ));
    }

    private void getRelatesForRoleLabel(RelationType relationType, String roleLabel, Transaction.Req request) {
        RoleType roleType = relationType.getRelates(roleLabel);
        ConceptProto.RelationType.GetRelatesForRoleLabel.Res.Builder getRelatesRes =
                ConceptProto.RelationType.GetRelatesForRoleLabel.Res.newBuilder();
        if (roleType != null) getRelatesRes.setRoleType(type(roleType));
        ConceptProto.Type.Res.Builder response =
                ConceptProto.Type.Res.newBuilder().setRelationTypeGetRelatesForRoleLabelRes(getRelatesRes);
        transactionSrv.respond(response(request, response));
    }

    private void setRelates(RelationType relationType, ConceptProto.RelationType.SetRelates.Req setRelatesReq,
                            Transaction.Req request) {
        if (setRelatesReq.getOverriddenCase() == OVERRIDDEN_LABEL) {
            relationType.setRelates(setRelatesReq.getLabel(), setRelatesReq.getOverriddenLabel());
        } else {
            relationType.setRelates(setRelatesReq.getLabel());
        }
        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setRelationTypeSetRelatesRes(
                ConceptProto.RelationType.SetRelates.Res.getDefaultInstance()
        )));
    }

    private void unsetRelates(RelationType relationType, ConceptProto.RelationType.UnsetRelates.Req unsetRelatesReq,
                              Transaction.Req request) {
        relationType.unsetRelates(unsetRelatesReq.getLabel());
        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setRelationTypeUnsetRelatesRes(
                ConceptProto.RelationType.UnsetRelates.Res.getDefaultInstance()
        )));
    }

    private void getRelationType(RoleType roleType, Transaction.Req request) {
        transactionSrv.respond(response(request, ConceptProto.Type.Res.newBuilder().setRoleTypeGetRelationTypeRes(
                ConceptProto.RoleType.GetRelationType.Res.newBuilder().setRelationType(type(roleType.getRelationType()))
        )));
    }

    private void getRelationTypes(RoleType roleType, Transaction.Req request) {
        transactionSrv.stream(roleType.getRelationTypes().iterator(), request.getId(), cons -> response(
                request, ConceptProto.Type.Res.newBuilder().setRoleTypeGetRelationTypesRes(
                        ConceptProto.RoleType.GetRelationTypes.Res.newBuilder().addAllRelationTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(toList())))
        ));
    }

    private void getPlayers(RoleType roleType, Transaction.Req request) {
        transactionSrv.stream(roleType.getPlayers().iterator(), request.getId(), cons -> response(
                request, ConceptProto.Type.Res.newBuilder().setRoleTypeGetPlayersRes(
                        ConceptProto.RoleType.GetPlayers.Res.newBuilder().addAllThingTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(toList())))
        ));
    }
}
