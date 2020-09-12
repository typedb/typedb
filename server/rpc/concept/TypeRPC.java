/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.core.Grakn;
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.type.Type;
import grakn.core.server.rpc.TransactionRPC;
import grakn.core.server.rpc.util.ResponseBuilder;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;

public class TypeRPC {

    private final Type type;
    private final Grakn.Transaction transaction;
    private final TransactionRPC.Iterators iterators;
    private final Consumer<TransactionProto.Transaction.Res> responder;

    public TypeRPC(Grakn.Transaction transaction, String label, @Nullable String scope, TransactionRPC.Iterators iterators,
                   Consumer<TransactionProto.Transaction.Res> responder) {
        this.transaction = transaction;
        this.iterators = iterators;
        this.responder = responder;
        this.type = scope != null && !scope.isEmpty() ?
                notNull(transaction.concepts().getRelationType(scope)).getRelates(label) :
                notNull(transaction.concepts().getType(label));
    }

    private static TransactionProto.Transaction.Res response(ConceptProto.TypeMethod.Res response) {
        return TransactionProto.Transaction.Res.newBuilder().setTypeMethodRes(response).build();
    }

    private static <T extends Type> T notNull(@Nullable T type) {
        if (type == null) throw new GraknException(MISSING_CONCEPT);
        return type;
    }

    public void execute(ConceptProto.TypeMethod.Req req) {
        switch (req.getReqCase()) {
            case TYPE_DELETE_REQ:
                this.delete();
                return;
            case TYPE_SETLABEL_REQ:
                this.setLabel(req.getTypeSetLabelReq().getLabel());
                return;
            case TYPE_ISABSTRACT_REQ:
                this.isAbstract();
                return;
            case TYPE_GETSUPERTYPE_REQ:
                this.getSupertype();
                return;
            case TYPE_SETSUPERTYPE_REQ:
                this.setSupertype(req.getTypeSetSupertypeReq().getType());
                return;
            case ROLETYPE_GETRELATION_REQ:
                this.asRoleType().getRelation();
                return;
            case THINGTYPE_SETABSTRACT_REQ:
                this.asThingType().setAbstract();
                return;
            case THINGTYPE_UNSETABSTRACT_REQ:
                this.asThingType().unsetAbstract();
                return;
            case THINGTYPE_SETOWNS_REQ:
                this.asThingType().setOwns(req.getThingTypeSetOwnsReq());
                return;
            case THINGTYPE_SETPLAYS_REQ:
                this.asThingType().setPlays(req.getThingTypeSetPlaysReq());
                return;
            case THINGTYPE_UNSETOWNS_REQ:
                this.asThingType().unsetOwns(req.getThingTypeUnsetOwnsReq().getAttributeType());
                return;
            case THINGTYPE_UNSETPLAYS_REQ:
                this.asThingType().unsetPlays(req.getThingTypeUnsetPlaysReq().getRole());
                return;
            case ENTITYTYPE_CREATE_REQ:
                this.asEntityType().create();
                return;
            case RELATIONTYPE_CREATE_REQ:
                this.asRelationType().create();
                return;
            case RELATIONTYPE_GETRELATESFORROLELABEL_REQ:
                this.asRelationType().getRelates(req.getRelationTypeGetRelatesForRoleLabelReq().getLabel());
                return;
            case RELATIONTYPE_SETRELATES_REQ:
                this.asRelationType().setRelates(req.getRelationTypeSetRelatesReq());
                return;
            case RELATIONTYPE_UNSETRELATES_REQ:
                this.asRelationType().unsetRelates(req.getRelationTypeUnsetRelatesReq());
                return;
            case ATTRIBUTETYPE_PUT_REQ:
                this.asAttributeType().put(req.getAttributeTypePutReq().getValue());
                return;
            case ATTRIBUTETYPE_GET_REQ:
                this.asAttributeType().get(req.getAttributeTypeGetReq().getValue());
                return;
            case ATTRIBUTETYPE_GETREGEX_REQ:
                this.asAttributeType().getRegex();
                return;
            case ATTRIBUTETYPE_SETREGEX_REQ:
                this.asAttributeType().setRegex(req.getAttributeTypeSetRegexReq().getRegex());
                return;
            case REQ_NOT_SET:
            default:
                throw new GraknException(UNKNOWN_REQUEST_TYPE);
        }
    }

    public void iterate(ConceptProto.TypeMethod.Iter.Req req) {
        switch (req.getReqCase()) {
            case TYPE_GETSUPERTYPES_ITER_REQ:
                this.getSupertypes();
                return;
            case TYPE_GETSUBTYPES_ITER_REQ:
                this.getSubtypes();
                return;
            case ROLETYPE_GETRELATIONS_ITER_REQ:
                this.asRoleType().getRelations();
                return;
            case ROLETYPE_GETPLAYERS_ITER_REQ:
                this.asRoleType().getPlayers();
                return;
            case THINGTYPE_GETINSTANCES_ITER_REQ:
                this.asThingType().getInstances();
                return;
            case THINGTYPE_GETOWNS_ITER_REQ:
                this.asThingType().getOwns(req.getThingTypeGetOwnsIterReq().getKeysOnly());
                return;
            case THINGTYPE_GETPLAYS_ITER_REQ:
                this.asThingType().getPlays();
                return;
            case RELATIONTYPE_GETRELATES_ITER_REQ:
                this.asRelationType().getRelates();
                return;
            case ATTRIBUTETYPE_GETOWNERS_ITER_REQ:
                this.asAttributeType().getOwners(req.getAttributeTypeGetOwnersIterReq().getOnlyKey());
                return;
            case REQ_NOT_SET:
            default:
                throw new GraknException(UNKNOWN_REQUEST_TYPE);
        }
    }

    private Type convertType(ConceptProto.Type protoType) {
        return transaction.concepts().getType(protoType.getLabel());
    }

    private grakn.core.concept.type.RoleType convertRoleType(ConceptProto.Type protoRole) {
        final Type type = transaction.concepts().getRelationType(protoRole.getScope()).getRelates(protoRole.getLabel());
        return type != null ? type.asRoleType() : null;
    }

    private ThingType asThingType() {
        return new ThingType();
    }

    private EntityType asEntityType() {
        return new EntityType();
    }

    private AttributeType asAttributeType() {
        return new AttributeType();
    }

    private RelationType asRelationType() {
        return new RelationType();
    }

    private RoleType asRoleType() {
        return new RoleType();
    }

    private void delete() {
        type.delete();
        responder.accept(null);
    }

    private void setLabel(final String label) {
        type.setLabel(label);
        responder.accept(null);
    }

    private void isAbstract() {
        final boolean isAbstract = type.isAbstract();

        final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                .setTypeIsAbstractRes(ConceptProto.Type.IsAbstract.Res.newBuilder()
                                              .setAbstract(isAbstract)).build();

        responder.accept(response(response));
    }

    private void getSupertype() {
        final Type supertype = type.getSupertype();
        final ConceptProto.Type.GetSupertype.Res.Builder responseConcept = ConceptProto.Type.GetSupertype.Res.newBuilder();
        if (supertype != null) responseConcept.setType(ResponseBuilder.Concept.type(supertype));

        ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                .setTypeGetSupertypeRes(responseConcept).build();

        responder.accept(response(response));
    }

    private void setSupertype(final ConceptProto.Type supertype) {
        // Make the second argument the super of the first argument

        final Type sup = convertType(supertype);

        if (type instanceof grakn.core.concept.type.EntityType) {
            type.asEntityType().setSupertype(sup.asEntityType());
        } else if (type instanceof grakn.core.concept.type.RelationType) {
            type.asRelationType().setSupertype(sup.asRelationType());
        } else if (type instanceof grakn.core.concept.type.AttributeType) {
            type.asAttributeType().setSupertype(sup.asAttributeType());
        }

        responder.accept(null);
    }

    private void getSupertypes() {
        final Stream<? extends Type> supertypes = type.getSupertypes();

        final Stream<TransactionProto.Transaction.Res> responses = supertypes.map(con -> {
            final ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                    .setTypeGetSupertypesIterRes(ConceptProto.Type.GetSupertypes.Iter.Res.newBuilder()
                                                         .setType(ResponseBuilder.Concept.type(con))).build();
            return ResponseBuilder.Transaction.Iter.typeMethod(res);
        });

        iterators.startBatchIterating(responses.iterator());
    }

    private void getSubtypes() {
        final Stream<? extends Type> subtypes = type.getSubtypes();

        final Stream<TransactionProto.Transaction.Res> responses = subtypes.map(con -> {
            final ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                    .setTypeGetSubtypesIterRes(ConceptProto.Type.GetSubtypes.Iter.Res.newBuilder()
                                                       .setType(ResponseBuilder.Concept.type(con))).build();
            return ResponseBuilder.Transaction.Iter.typeMethod(res);
        });

        iterators.startBatchIterating(responses.iterator());
    }

    private class ThingType {

        private final grakn.core.concept.type.ThingType thingType = TypeRPC.this.type.asType().asThingType();

        private void getInstances() {
            Stream<? extends grakn.core.concept.thing.Thing> concepts = thingType.getInstances();

            Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                        .setThingTypeGetInstancesIterRes(ConceptProto.ThingType.GetInstances.Iter.Res.newBuilder()
                                                                 .setThing(ResponseBuilder.Concept.thing(con))).build();
                return ResponseBuilder.Transaction.Iter.typeMethod(res);
            });

            iterators.startBatchIterating(responses.iterator());
        }

        private void setAbstract() {
            thingType.setAbstract();
            responder.accept(null);
        }

        private void unsetAbstract() {
            thingType.unsetAbstract();
            responder.accept(null);
        }

        private void getOwns(final boolean keysOnly) {
            final Stream<? extends grakn.core.concept.type.AttributeType> ownedTypes = thingType.getOwns(keysOnly);

            final Stream<TransactionProto.Transaction.Res> responses = ownedTypes.map(con -> {
                final ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                        .setThingTypeGetOwnsIterRes(ConceptProto.ThingType.GetOwns.Iter.Res.newBuilder()
                                                            .setAttributeType(ResponseBuilder.Concept.type(con))).build();
                return ResponseBuilder.Transaction.Iter.typeMethod(res);
            });

            iterators.startBatchIterating(responses.iterator());
        }

        private void getPlays() {
            final Stream<? extends grakn.core.concept.type.RoleType> roleTypes = thingType.getPlays();

            final Stream<TransactionProto.Transaction.Res> responses = roleTypes.map(con -> {
                final ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                        .setThingTypeGetPlaysIterRes(ConceptProto.ThingType.GetPlays.Iter.Res.newBuilder()
                                                             .setRole(ResponseBuilder.Concept.type(con))).build();
                return ResponseBuilder.Transaction.Iter.typeMethod(res);
            });

            iterators.startBatchIterating(responses.iterator());
        }

        private void setOwns(final ConceptProto.ThingType.SetOwns.Req req) {
            final grakn.core.concept.type.AttributeType attributeType = convertType(req.getAttributeType()).asAttributeType();
            final boolean isKey = req.getIsKey();

            if (req.hasOverriddenType()) {
                final grakn.core.concept.type.AttributeType overriddenType = convertType(req.getOverriddenType()).asAttributeType();
                thingType.setOwns(attributeType, overriddenType, isKey);
            } else {
                thingType.setOwns(attributeType, isKey);
            }
            responder.accept(null);
        }

        private void setPlays(final ConceptProto.ThingType.SetPlays.Req request) {
            final grakn.core.concept.type.RoleType role = convertRoleType(request.getRole());
            if (request.hasOverriddenRole()) {
                final grakn.core.concept.type.RoleType overriddenRole = convertRoleType(request.getOverriddenRole());
                thingType.setPlays(role, overriddenRole);
            } else {
                thingType.setPlays(role);
            }
            responder.accept(null);
        }

        private void unsetOwns(final ConceptProto.Type protoAttributeType) {
            final grakn.core.concept.type.AttributeType attributeType = convertType(protoAttributeType).asAttributeType();
            thingType.unsetOwns(attributeType);
            responder.accept(null);
        }

        private void unsetPlays(final ConceptProto.Type protoRoleType) {
            final grakn.core.concept.type.RoleType role = convertRoleType(protoRoleType);
            thingType.unsetPlays(role);
            responder.accept(null);
        }
    }

    private class EntityType {

        private final grakn.core.concept.type.EntityType entityType = type.asType().asEntityType();

        private void create() {
            final Entity entity = entityType.create();

            final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                    .setEntityTypeCreateRes(ConceptProto.EntityType.Create.Res.newBuilder()
                                                    .setEntity(ResponseBuilder.Concept.thing(entity))).build();

            responder.accept(response(response));
        }
    }

    private class AttributeType {

        private final grakn.core.concept.type.AttributeType attributeType = TypeRPC.this.type.asType().asAttributeType();

        private void getOwners(final boolean onlyKey) {
            final Stream<? extends grakn.core.concept.type.ThingType> owners = attributeType.getOwners(onlyKey);

            final Stream<TransactionProto.Transaction.Res> responses = owners.map(thingType -> {
                final ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                        .setAttributeTypeGetOwnersIterRes(ConceptProto.AttributeType.GetOwners.Iter.Res.newBuilder()
                                                                  .setOwner(ResponseBuilder.Concept.type(thingType))).build();
                return ResponseBuilder.Transaction.Iter.typeMethod(res);
            });

            iterators.startBatchIterating(responses.iterator());
        }

        private void put(final ConceptProto.Attribute.Value protoValue) {
            final grakn.core.concept.thing.Attribute attribute;
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
                case DATETIME:
                    attribute = attributeType.asDateTime().put(Instant.ofEpochMilli(protoValue.getDatetime()).atOffset(ZoneOffset.UTC).toLocalDateTime());
                    break;
                case BOOLEAN:
                    attribute = attributeType.asBoolean().put(protoValue.getBoolean());
                    break;
                case VALUE_NOT_SET:
                default:
                    throw new GraknException(ErrorMessage.Server.BAD_VALUE_TYPE);
            }

            final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                    .setAttributeTypePutRes(ConceptProto.AttributeType.Put.Res.newBuilder()
                                                    .setAttribute(ResponseBuilder.Concept.thing(attribute))).build();

            responder.accept(response(response));
        }

        private void get(final ConceptProto.Attribute.Value protoValue) {
            final grakn.core.concept.thing.Attribute attribute;
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
                case DATETIME:
                    attribute = attributeType.asDateTime().get(Instant.ofEpochMilli(protoValue.getDatetime()).atOffset(ZoneOffset.UTC).toLocalDateTime());
                    break;
                case BOOLEAN:
                    attribute = attributeType.asBoolean().get(protoValue.getBoolean());
                    break;
                case VALUE_NOT_SET:
                default:
                    throw new GraknException(ErrorMessage.Server.BAD_VALUE_TYPE);
            }

            final ConceptProto.AttributeType.Get.Res.Builder methodResponse = ConceptProto.AttributeType.Get.Res.newBuilder();
            if (attribute != null) methodResponse.setAttribute(ResponseBuilder.Concept.thing(attribute)).build();

            final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                    .setAttributeTypeGetRes(methodResponse).build();

            responder.accept(response(response));
        }

        private void getRegex() {
            final Pattern regex = attributeType.asString().getRegex();
            final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                    .setAttributeTypeGetRegexRes(ConceptProto.AttributeType.GetRegex.Res.newBuilder()
                                                         .setRegex((regex != null) ? regex.pattern() : "")).build();
            responder.accept(response(response));
        }

        private void setRegex(String regex) {
            if (regex.isEmpty()) attributeType.asString().setRegex(null);
            else attributeType.asString().setRegex(Pattern.compile(regex));
            responder.accept(null);
        }
    }

    private class RelationType {
        private final grakn.core.concept.type.RelationType relationType = TypeRPC.this.type.asType().asRelationType();

        private void create() {
            final grakn.core.concept.thing.Relation relation = relationType.asRelationType().create();
            final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                    .setRelationTypeCreateRes(ConceptProto.RelationType.Create.Res.newBuilder()
                                                      .setRelation(ResponseBuilder.Concept.thing(relation))).build();
            responder.accept(response(response));
        }

        private void getRelates() {
            final Stream<? extends grakn.core.concept.type.RoleType> roleTypes = relationType.getRelates();
            final Stream<TransactionProto.Transaction.Res> responses = roleTypes.map(con -> {
                final ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                        .setRelationTypeGetRelatesIterRes(ConceptProto.RelationType.GetRelates.Iter.Res.newBuilder()
                                                                  .setRole(ResponseBuilder.Concept.type(con))).build();
                return ResponseBuilder.Transaction.Iter.typeMethod(res);
            });
            iterators.startBatchIterating(responses.iterator());
        }

        private void getRelates(final String label) {
            final grakn.core.concept.type.RoleType roleType = relationType.getRelates(label);
            final ConceptProto.RelationType.GetRelatesForRoleLabel.Res.Builder builder =
                    ConceptProto.RelationType.GetRelatesForRoleLabel.Res.newBuilder();
            if (roleType != null) builder.setRoleType(ResponseBuilder.Concept.type(roleType));
            final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                    .setRelationTypeGetRelatesForRoleLabelRes(builder).build();
            responder.accept(response(response));
        }

        private void setRelates(final ConceptProto.RelationType.SetRelates.Req request) {
            if (request.getOverriddenCase() == ConceptProto.RelationType.SetRelates.Req.OverriddenCase.OVERRIDDENLABEL) {
                relationType.setRelates(request.getLabel(), request.getOverriddenLabel());
            } else {
                relationType.setRelates(request.getLabel());
            }
            final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                    .setRelationTypeSetRelatesRes(ConceptProto.RelationType.SetRelates.Res.getDefaultInstance()).build();
            responder.accept(response(response));
        }

        private void unsetRelates(final ConceptProto.RelationType.UnsetRelates.Req request) {
            relationType.unsetRelates(request.getLabel());
            final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                    .setRelationTypeUnsetRelatesRes(ConceptProto.RelationType.UnsetRelates.Res.getDefaultInstance()).build();
            responder.accept(response(response));
        }
    }

    private class RoleType {

        private final grakn.core.concept.type.RoleType roleType = TypeRPC.this.type.asType().asRoleType();

        private void getRelation() {
            final grakn.core.concept.type.RelationType relationType = roleType.getRelation();
            final ConceptProto.TypeMethod.Res response = ConceptProto.TypeMethod.Res.newBuilder()
                    .setRoleTypeGetRelationRes(ConceptProto.RoleType.GetRelation.Res.newBuilder()
                                                       .setRelationType(ResponseBuilder.Concept.type(relationType))).build();
            responder.accept(response(response));
        }

        private void getRelations() {
            Stream<? extends grakn.core.concept.type.RelationType> relationTypes = roleType.getRelations();
            Stream<TransactionProto.Transaction.Res> responses = relationTypes.map(con -> {
                ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                        .setRoleTypeGetRelationsIterRes(ConceptProto.RoleType.GetRelations.Iter.Res.newBuilder()
                                                                .setRelationType(ResponseBuilder.Concept.type(con))).build();
                return ResponseBuilder.Transaction.Iter.typeMethod(res);
            });
            iterators.startBatchIterating(responses.iterator());
        }

        private void getPlayers() {
            Stream<? extends grakn.core.concept.type.ThingType> players = roleType.getPlayers();

            Stream<TransactionProto.Transaction.Res> responses = players.map(con -> {
                ConceptProto.TypeMethod.Iter.Res res = ConceptProto.TypeMethod.Iter.Res.newBuilder()
                        .setRoleTypeGetPlayersIterRes(ConceptProto.RoleType.GetPlayers.Iter.Res.newBuilder()
                                                              .setThingType(ResponseBuilder.Concept.type(con))).build();
                return ResponseBuilder.Transaction.Iter.typeMethod(res);
            });
            iterators.startBatchIterating(responses.iterator());
        }
    }
}
