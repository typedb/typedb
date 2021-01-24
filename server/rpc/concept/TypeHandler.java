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

package grakn.core.server.rpc.concept;

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
import grakn.core.server.rpc.TransactionRPC;
import grakn.core.server.rpc.common.ResponseBuilder;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto.Transaction;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static grakn.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static grakn.core.common.exception.ErrorMessage.Server.MISSING_FIELD;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.common.exception.ErrorMessage.TypeWrite.ILLEGAL_SUPERTYPE_ENCODING;
import static grakn.core.server.rpc.common.ResponseBuilder.Concept.thing;
import static grakn.core.server.rpc.common.ResponseBuilder.Concept.type;
import static grakn.core.server.rpc.common.ResponseBuilder.Concept.valueType;

public class TypeHandler {

    private final TransactionRPC transactionRPC;
    private final ConceptManager conceptManager;

    public TypeHandler(TransactionRPC transactionRPC, ConceptManager conceptManager) {
        this.transactionRPC = transactionRPC;
        this.conceptManager = conceptManager;
    }

    public void handleRequest(Transaction.Req request) {
        final ConceptProto.Type.Req typeReq = request.getTypeReq();
        final String label = typeReq.getLabel();
        final String scope = typeReq.getScope();
        final Type type = scope != null && !scope.isEmpty() ?
                notNull(conceptManager.getRelationType(scope)).getRelates(label) :
                notNull(conceptManager.getThingType(label));
        switch (typeReq.getReqCase()) {
            case TYPE_DELETE_REQ:
                delete(request, type);
                return;
            case TYPE_SET_LABEL_REQ:
                setLabel(request, type, typeReq.getTypeSetLabelReq().getLabel());
                return;
            case TYPE_IS_ABSTRACT_REQ:
                isAbstract(request, type);
                return;
            case TYPE_GET_SUPERTYPE_REQ:
                getSupertype(request, type);
                return;
            case TYPE_SET_SUPERTYPE_REQ:
                setSupertype(request, type, typeReq.getTypeSetSupertypeReq().getType());
                return;
            case TYPE_GET_SUPERTYPES_REQ:
                getSupertypes(request, type);
                return;
            case TYPE_GET_SUBTYPES_REQ:
                getSubtypes(request, type);
                return;
            case ROLE_TYPE_GET_RELATION_TYPE_REQ:
                getRelationType(request, type.asRoleType());
                return;
            case ROLE_TYPE_GET_RELATION_TYPES_REQ:
                getRelationTypes(request, type.asRoleType());
                return;
            case ROLE_TYPE_GET_PLAYERS_REQ:
                getPlayers(request, type.asRoleType());
                return;
            case THING_TYPE_SET_ABSTRACT_REQ:
                setAbstract(request, type.asThingType());
                return;
            case THING_TYPE_UNSET_ABSTRACT_REQ:
                unsetAbstract(request, type.asThingType());
                return;
            case THING_TYPE_SET_OWNS_REQ:
                setOwns(request, type.asThingType(), typeReq.getThingTypeSetOwnsReq());
                return;
            case THING_TYPE_SET_PLAYS_REQ:
                setPlays(request, type.asThingType(), typeReq.getThingTypeSetPlaysReq());
                return;
            case THING_TYPE_UNSET_OWNS_REQ:
                unsetOwns(request, type.asThingType(), typeReq.getThingTypeUnsetOwnsReq().getAttributeType());
                return;
            case THING_TYPE_UNSET_PLAYS_REQ:
                unsetPlays(request, type.asThingType(), typeReq.getThingTypeUnsetPlaysReq().getRole());
                return;
            case THING_TYPE_GET_INSTANCES_REQ:
                getInstances(request, type.asThingType());
                return;
            case THING_TYPE_GET_OWNS_REQ:
                if (typeReq.getThingTypeGetOwnsReq().getFilterCase() == ConceptProto.ThingType.GetOwns.Req.FilterCase.VALUE_TYPE) {
                    getOwns(request, type.asThingType(), valueType(typeReq.getThingTypeGetOwnsReq().getValueType()), typeReq.getThingTypeGetOwnsReq().getKeysOnly());
                } else {
                    getOwns(request, type.asThingType(), typeReq.getThingTypeGetOwnsReq().getKeysOnly());
                }
                return;
            case THING_TYPE_GET_PLAYS_REQ:
                getPlays(request, type.asThingType());
                return;
            case ENTITY_TYPE_CREATE_REQ:
                create(request, type.asEntityType());
                return;
            case RELATION_TYPE_CREATE_REQ:
                create(request, type.asRelationType());
                return;
            case RELATION_TYPE_GET_RELATES_FOR_ROLE_LABEL_REQ:
                getRelatesForRoleLabel(request, type.asRelationType(), typeReq.getRelationTypeGetRelatesForRoleLabelReq().getLabel());
                return;
            case RELATION_TYPE_SET_RELATES_REQ:
                setRelates(request, type.asRelationType(), typeReq.getRelationTypeSetRelatesReq());
                return;
            case RELATION_TYPE_UNSET_RELATES_REQ:
                unsetRelates(request, type.asRelationType(), typeReq.getRelationTypeUnsetRelatesReq());
                return;
            case RELATION_TYPE_GET_RELATES_REQ:
                getRelates(request, type.asRelationType());
                return;
            case ATTRIBUTE_TYPE_PUT_REQ:
                put(request, type.asAttributeType(), typeReq.getAttributeTypePutReq().getValue());
                return;
            case ATTRIBUTE_TYPE_GET_REQ:
                get(request, type.asAttributeType(), typeReq.getAttributeTypeGetReq().getValue());
                return;
            case ATTRIBUTE_TYPE_GET_REGEX_REQ:
                getRegex(request, type.asAttributeType());
                return;
            case ATTRIBUTE_TYPE_SET_REGEX_REQ:
                setRegex(request, type.asAttributeType(), typeReq.getAttributeTypeSetRegexReq().getRegex());
                return;
            case ATTRIBUTE_TYPE_GET_OWNERS_REQ:
                getOwners(request, type.asAttributeType(), typeReq.getAttributeTypeGetOwnersReq().getOnlyKey());
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
        return conceptManager.getThingType(protoType.getLabel());
    }

    private RoleType getRoleType(ConceptProto.Type protoRoleType) {
        RelationType relationType = conceptManager.getRelationType(protoRoleType.getScope());
        if (relationType != null) return relationType.getRelates(protoRoleType.getLabel());
        else return null;
    }

    private void delete(Transaction.Req request, Type type) {
        type.delete();
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setTypeDeleteRes(
                ConceptProto.Type.Delete.Res.getDefaultInstance()
        )));
    }

    private void setLabel(Transaction.Req request, Type type, String label) {
        type.setLabel(label);
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setTypeSetLabelRes(
                ConceptProto.Type.SetLabel.Res.getDefaultInstance()
        )));
    }

    private void isAbstract(Transaction.Req request, Type type) {
        final ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder()
                .setTypeIsAbstractRes(ConceptProto.Type.IsAbstract.Res.newBuilder().setAbstract(type.isAbstract()));
        transactionRPC.respond(response(request, response));
    }

    private void getSupertype(Transaction.Req request, Type type) {
        ConceptProto.Type.GetSupertype.Res.Builder getSupertypeRes = ConceptProto.Type.GetSupertype.Res.newBuilder();
        Type superType = type.getSupertype();
        if (superType != null) getSupertypeRes.setType(type(superType));
        final ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder()
                .setTypeGetSupertypeRes(getSupertypeRes);
        transactionRPC.respond(response(request, response));
    }

    private void setSupertype(Transaction.Req request, Type type, ConceptProto.Type supertype) {
        final Type sup = getThingType(supertype);

        if (type instanceof EntityType) {
            type.asEntityType().setSupertype(sup.asEntityType());
        } else if (type instanceof RelationType) {
            type.asRelationType().setSupertype(sup.asRelationType());
        } else if (type instanceof AttributeType) {
            type.asAttributeType().setSupertype(sup.asAttributeType());
        } else {
            throw GraknException.of(ILLEGAL_SUPERTYPE_ENCODING, className(type.getClass()));
        }

        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setTypeSetSupertypeRes(
                ConceptProto.Type.SetSupertype.Res.getDefaultInstance()
        )));
    }

    private void getSupertypes(Transaction.Req request, Type type) {
        transactionRPC.respond(request, type.getSupertypes().iterator(), cons ->
                response(request, ConceptProto.Type.Res.newBuilder().setTypeGetSupertypesRes(
                        ConceptProto.Type.GetSupertypes.Res.newBuilder().addAllTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(Collectors.toList())))));
    }

    private void getSubtypes(Transaction.Req request, Type type) {
        transactionRPC.respond(
                request, type.getSubtypes().iterator(),
                cons -> response(request, ConceptProto.Type.Res.newBuilder().setTypeGetSubtypesRes(
                        ConceptProto.Type.GetSubtypes.Res.newBuilder().addAllTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(Collectors.toList()))))
        );
    }

    private void getInstances(Transaction.Req request, ThingType thingType) {
        transactionRPC.respond(
                request, thingType.getInstances().iterator(),
                cons -> response(request, ConceptProto.Type.Res.newBuilder().setThingTypeGetInstancesRes(
                        ConceptProto.ThingType.GetInstances.Res.newBuilder().addAllThings(
                                cons.stream().map(ResponseBuilder.Concept::thing).collect(Collectors.toList()))))
        );
    }

    private void setAbstract(Transaction.Req request, ThingType thingType) {
        thingType.setAbstract();
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setThingTypeSetAbstractRes(
                ConceptProto.ThingType.SetAbstract.Res.getDefaultInstance()
        )));
    }

    private void unsetAbstract(Transaction.Req request, ThingType thingType) {
        thingType.unsetAbstract();
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setThingTypeUnsetAbstractRes(
                ConceptProto.ThingType.UnsetAbstract.Res.getDefaultInstance()
        )));
    }

    private void getOwns(Transaction.Req request, ThingType thingType, boolean keysOnly) {
        transactionRPC.respond(
                request, thingType.getOwns(keysOnly).iterator(),
                cons -> response(request, ConceptProto.Type.Res.newBuilder().setThingTypeGetOwnsRes(
                        ConceptProto.ThingType.GetOwns.Res.newBuilder().addAllAttributeTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(Collectors.toList()))))
        );
    }

    private void getOwns(Transaction.Req request, ThingType thingType, AttributeType.ValueType valueType, boolean keysOnly) {
        transactionRPC.respond(
                request, thingType.getOwns(valueType, keysOnly).iterator(),
                cons -> response(request, ConceptProto.Type.Res.newBuilder().setThingTypeGetOwnsRes(
                        ConceptProto.ThingType.GetOwns.Res.newBuilder().addAllAttributeTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(Collectors.toList()))))
        );
    }

    private void getPlays(Transaction.Req request, ThingType thingType) {
        transactionRPC.respond(
                request, thingType.getPlays().iterator(),
                cons -> response(request, ConceptProto.Type.Res.newBuilder().setThingTypeGetPlaysRes(
                        ConceptProto.ThingType.GetPlays.Res.newBuilder().addAllRoles(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(Collectors.toList()))))
        );
    }

    private void setOwns(Transaction.Req request, ThingType thingType, ConceptProto.ThingType.SetOwns.Req req) {
        final AttributeType attributeType = getThingType(req.getAttributeType()).asAttributeType();
        final boolean isKey = req.getIsKey();

        if (req.hasOverriddenType()) {
            final AttributeType overriddenType = getThingType(req.getOverriddenType()).asAttributeType();
            thingType.setOwns(attributeType, overriddenType, isKey);
        } else {
            thingType.setOwns(attributeType, isKey);
        }
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setThingTypeSetOwnsRes(
                ConceptProto.ThingType.SetOwns.Res.getDefaultInstance()
        )));
    }

    private void setPlays(Transaction.Req request, ThingType thingType, ConceptProto.ThingType.SetPlays.Req setPlaysReq) {
        final RoleType role = getRoleType(setPlaysReq.getRole());
        if (setPlaysReq.hasOverriddenRole()) {
            final RoleType overriddenRole = getRoleType(setPlaysReq.getOverriddenRole());
            thingType.setPlays(role, overriddenRole);
        } else {
            thingType.setPlays(role);
        }
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setThingTypeSetPlaysRes(
                ConceptProto.ThingType.SetPlays.Res.getDefaultInstance()
        )));
    }

    private void unsetOwns(Transaction.Req request, ThingType thingType, ConceptProto.Type protoAttributeType) {
        final AttributeType attributeType = getThingType(protoAttributeType).asAttributeType();
        thingType.unsetOwns(attributeType);
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setThingTypeUnsetOwnsRes(
                ConceptProto.ThingType.UnsetOwns.Res.getDefaultInstance()
        )));
    }

    private void unsetPlays(Transaction.Req request, ThingType thingType, ConceptProto.Type protoRoleType) {
        final RoleType role = notNull(getRoleType(protoRoleType));
        thingType.unsetPlays(role);
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setThingTypeUnsetPlaysRes(
                ConceptProto.ThingType.UnsetPlays.Res.getDefaultInstance()
        )));
    }

    private void create(Transaction.Req request, EntityType entityType) {
        final Entity entity = entityType.create();
        final ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder()
                .setEntityTypeCreateRes(ConceptProto.EntityType.Create.Res.newBuilder().setEntity(thing(entity)));
        transactionRPC.respond(response(request, response));
    }

    private void getOwners(Transaction.Req request, AttributeType attributeType, boolean onlyKey) {
        transactionRPC.respond(
                request, attributeType.getOwners(onlyKey).iterator(),
                cons -> response(request, ConceptProto.Type.Res.newBuilder().setAttributeTypeGetOwnersRes(
                        ConceptProto.AttributeType.GetOwners.Res.newBuilder().addAllOwners(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(Collectors.toList()))))
        );
    }

    private void put(Transaction.Req request, AttributeType attributeType, ConceptProto.Attribute.Value protoValue) {
        final Attribute attribute;
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
                attribute = attributeType.asDateTime().put(
                        Instant.ofEpochMilli(protoValue.getDateTime()).atOffset(ZoneOffset.UTC).toLocalDateTime()
                );
                break;
            case BOOLEAN:
                attribute = attributeType.asBoolean().put(protoValue.getBoolean());
                break;
            case VALUE_NOT_SET:
                throw GraknException.of(MISSING_FIELD, "value");
            default:
                throw GraknException.of(BAD_VALUE_TYPE, protoValue.getValueCase());
        }

        final ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder()
                .setAttributeTypePutRes(ConceptProto.AttributeType.Put.Res.newBuilder().setAttribute(thing(attribute)));
        transactionRPC.respond(response(request, response));
    }

    private void get(Transaction.Req request, AttributeType attributeType, ConceptProto.Attribute.Value protoValue) {
        final Attribute attribute;
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
                attribute = attributeType.asDateTime().get(
                        Instant.ofEpochMilli(protoValue.getDateTime()).atOffset(ZoneOffset.UTC).toLocalDateTime()
                );
                break;
            case BOOLEAN:
                attribute = attributeType.asBoolean().get(protoValue.getBoolean());
                break;
            case VALUE_NOT_SET:
            default:
                // TODO: Unify our exceptions - they should either all be GraknException or all be StatusRuntimeException
                throw GraknException.of(BAD_VALUE_TYPE);
        }

        final ConceptProto.AttributeType.Get.Res.Builder getAttributeTypeRes = ConceptProto.AttributeType.Get.Res.newBuilder();
        if (attribute != null) getAttributeTypeRes.setAttribute(thing(attribute));
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setAttributeTypeGetRes(getAttributeTypeRes)));
    }

    private void getRegex(Transaction.Req request, AttributeType attributeType) {
        final Pattern regex = attributeType.asString().getRegex();
        final ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder().setAttributeTypeGetRegexRes(
                ConceptProto.AttributeType.GetRegex.Res.newBuilder().setRegex((regex != null) ? regex.pattern() : "")
        );
        transactionRPC.respond(response(request, response));
    }

    private void setRegex(Transaction.Req request, AttributeType attributeType, String regex) {
        if (regex.isEmpty()) attributeType.asString().setRegex(null);
        else attributeType.asString().setRegex(Pattern.compile(regex));
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setAttributeTypeSetRegexRes(
                ConceptProto.AttributeType.SetRegex.Res.getDefaultInstance())
        ));
    }

    private void create(Transaction.Req request, RelationType relationType) {
        final Relation relation = relationType.create();
        final ConceptProto.Type.Res.Builder response = ConceptProto.Type.Res.newBuilder().setRelationTypeCreateRes(
                ConceptProto.RelationType.Create.Res.newBuilder().setRelation(thing(relation))
        );
        transactionRPC.respond(response(request, response));
    }

    private void getRelates(Transaction.Req request, RelationType relationType) {
        transactionRPC.respond(
                request, relationType.getRelates().iterator(),
                cons -> response(request, ConceptProto.Type.Res.newBuilder().setRelationTypeGetRelatesRes(
                        ConceptProto.RelationType.GetRelates.Res.newBuilder().addAllRoles(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(Collectors.toList()))))
        );
    }

    private void getRelatesForRoleLabel(Transaction.Req request, RelationType relationType, String roleLabel) {
        final RoleType roleType = relationType.getRelates(roleLabel);
        final ConceptProto.RelationType.GetRelatesForRoleLabel.Res.Builder getRelatesRes = ConceptProto.RelationType.GetRelatesForRoleLabel.Res.newBuilder();
        if (roleType != null) getRelatesRes.setRoleType(type(roleType));
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setRelationTypeGetRelatesForRoleLabelRes(getRelatesRes)));
    }

    private void setRelates(Transaction.Req request, RelationType relationType, ConceptProto.RelationType.SetRelates.Req setRelatesReq) {
        if (setRelatesReq.getOverriddenCase() == ConceptProto.RelationType.SetRelates.Req.OverriddenCase.OVERRIDDEN_LABEL) {
            relationType.setRelates(setRelatesReq.getLabel(), setRelatesReq.getOverriddenLabel());
        } else {
            relationType.setRelates(setRelatesReq.getLabel());
        }
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setRelationTypeSetRelatesRes(
                ConceptProto.RelationType.SetRelates.Res.getDefaultInstance()
        )));
    }

    private void unsetRelates(Transaction.Req request, RelationType relationType, ConceptProto.RelationType.UnsetRelates.Req unsetRelatesReq) {
        relationType.unsetRelates(unsetRelatesReq.getLabel());
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setRelationTypeUnsetRelatesRes(
                ConceptProto.RelationType.UnsetRelates.Res.getDefaultInstance()
        )));
    }

    private void getRelationType(Transaction.Req request, RoleType roleType) {
        transactionRPC.respond(response(request, ConceptProto.Type.Res.newBuilder().setRoleTypeGetRelationTypeRes(
                ConceptProto.RoleType.GetRelationType.Res.newBuilder().setRelationType(type(roleType.getRelationType()))
        )));
    }

    private void getRelationTypes(Transaction.Req request, RoleType roleType) {
        transactionRPC.respond(
                request, roleType.getRelationTypes().iterator(),
                cons -> response(request, ConceptProto.Type.Res.newBuilder().setRoleTypeGetRelationTypesRes(
                        ConceptProto.RoleType.GetRelationTypes.Res.newBuilder().addAllRelationTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(Collectors.toList()))))
        );
    }

    private void getPlayers(Transaction.Req request, RoleType roleType) {
        transactionRPC.respond(
                request, roleType.getPlayers().iterator(),
                cons -> response(request, ConceptProto.Type.Res.newBuilder().setRoleTypeGetPlayersRes(
                        ConceptProto.RoleType.GetPlayers.Res.newBuilder().addAllThingTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(Collectors.toList()))))
        );
    }
}
