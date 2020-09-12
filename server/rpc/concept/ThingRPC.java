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

import com.google.protobuf.ByteString;
import grakn.core.Grakn;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.Concept;
import grakn.core.concept.thing.Thing;
import grakn.core.server.rpc.TransactionRPC;
import grakn.core.server.rpc.util.ResponseBuilder;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;

public class ThingRPC {

    private final Thing thing;
    private final grakn.core.Grakn.Transaction transaction;
    private final TransactionRPC.Iterators iterators;
    private final Consumer<TransactionProto.Transaction.Res> responder;

    public ThingRPC(Grakn.Transaction transaction, ByteString iid, TransactionRPC.Iterators iterators,
                    Consumer<TransactionProto.Transaction.Res> responder) {
        this.thing = notNull(transaction.concepts().getThing(iid.toByteArray()));
        this.transaction = transaction;
        this.iterators = iterators;
        this.responder = responder;
    }

    private static TransactionProto.Transaction.Res response(ConceptProto.ThingMethod.Res response) {
        return TransactionProto.Transaction.Res.newBuilder().setThingMethodRes(response).build();
    }

    private static <T extends Concept> T notNull(@Nullable T concept) {
        if (concept == null) throw new GraknException(MISSING_CONCEPT);
        return concept;
    }

    public void execute(ConceptProto.ThingMethod.Req req) {
        switch (req.getReqCase()) {
            case THING_DELETE_REQ:
                this.delete();
                return;
            case THING_GETTYPE_REQ:
                this.getType();
                return;
            case THING_ISINFERRED_REQ:
                this.isInferred();
                return;
            case THING_SETHAS_REQ:
                this.setHas(req.getThingSetHasReq().getAttribute());
                return;
            case THING_UNSETHAS_REQ:
                this.unsetHas(req.getThingUnsetHasReq().getAttribute());
                return;
            case RELATION_ADDPLAYER_REQ:
                this.asRelation().addPlayer(req.getRelationAddPlayerReq());
                return;
            case RELATION_REMOVEPLAYER_REQ:
                this.asRelation().removePlayer(req.getRelationRemovePlayerReq());
                return;
            case REQ_NOT_SET:
            default:
                throw new GraknException(UNKNOWN_REQUEST_TYPE);
        }
    }

    public void iterate(ConceptProto.ThingMethod.Iter.Req req) {
        switch (req.getReqCase()) {
            case THING_GETHAS_ITER_REQ:
                this.getHas(req.getThingGetHasIterReq());
                return;
            case THING_GETRELATIONS_ITER_REQ:
                this.getRelations(req.getThingGetRelationsIterReq().getRoleTypesList());
                return;
            case THING_GETPLAYS_ITER_REQ:
                this.getPlays();
                return;
            case RELATION_GETPLAYERS_ITER_REQ:
                this.asRelation().getPlayers(req.getRelationGetPlayersIterReq().getRoleTypesList());
                return;
            case RELATION_GETPLAYERSBYROLETYPE_ITER_REQ:
                this.asRelation().getPlayersByRoleType();
                return;
            case ATTRIBUTE_GETOWNERS_ITER_REQ:
                this.asAttribute().getOwners(req.getAttributeGetOwnersIterReq());
                return;
            case REQ_NOT_SET:
            default:
                throw new GraknException(UNKNOWN_REQUEST_TYPE);
        }
    }

    private Thing getThing(ConceptProto.Thing protoThing) {
        return transaction.concepts().getThing(protoThing.getIid().toByteArray());
    }

    private grakn.core.concept.type.Type getType(ConceptProto.Type protoType) {
        return transaction.concepts().getType(protoType.getLabel());
    }

    private grakn.core.concept.type.RoleType getRoleType(ConceptProto.Type protoRole) {
        return transaction.concepts().getRelationType(protoRole.getScope()).getRelates(protoRole.getLabel());
    }

    private Relation asRelation() {
        return new Relation();
    }

    private Attribute asAttribute() {
        return new Attribute();
    }

    private void delete() {
        thing.delete();
        responder.accept(null);
    }

    private void isInferred() {
        boolean inferred = thing.isInferred();
        ConceptProto.ThingMethod.Res response = ConceptProto.ThingMethod.Res.newBuilder()
                .setThingIsInferredRes(ConceptProto.Thing.IsInferred.Res.newBuilder()
                                               .setInferred(inferred)).build();
        responder.accept(response(response));
    }

    private void getType() {
        final grakn.core.concept.type.ThingType thingType = thing.getType();
        ConceptProto.ThingMethod.Res response = ConceptProto.ThingMethod.Res.newBuilder()
                .setThingGetTypeRes(ConceptProto.Thing.GetType.Res.newBuilder()
                                            .setThingType(ResponseBuilder.Concept.type(thingType))).build();
        responder.accept(response(response));
    }

    private void getHas(ConceptProto.Thing.GetHas.Iter.Req req) {
        final List<ConceptProto.Type> protoTypes = req.getAttributeTypesList();
        final Stream<? extends grakn.core.concept.thing.Attribute> attributes;

        if (protoTypes.isEmpty()) {
            attributes = thing.getHas(req.getKeysOnly());
        } else {
            final grakn.core.concept.type.AttributeType[] attributeTypes = protoTypes.stream()
                    .map(this::getType)
                    .map(ThingRPC::notNull)
                    .map(Concept::asType)
                    .map(grakn.core.concept.type.Type::asAttributeType)
                    .toArray(grakn.core.concept.type.AttributeType[]::new);
            attributes = thing.getHas(attributeTypes);
        }

        Stream<TransactionProto.Transaction.Res> responses = attributes.map(con -> {
            ConceptProto.ThingMethod.Iter.Res res = ConceptProto.ThingMethod.Iter.Res.newBuilder()
                    .setThingGetHasIterRes(ConceptProto.Thing.GetHas.Iter.Res.newBuilder()
                                                   .setAttribute(ResponseBuilder.Concept.thing(con))).build();
            return ResponseBuilder.Transaction.Iter.thingMethod(res);
        });

        iterators.startBatchIterating(responses.iterator());
    }

    private void getRelations(List<ConceptProto.Type> protoRoleTypes) {
        final grakn.core.concept.type.RoleType[] roles = protoRoleTypes.stream()
                .map(this::getRoleType)
                .map(ThingRPC::notNull)
                .toArray(grakn.core.concept.type.RoleType[]::new);
        Stream<? extends grakn.core.concept.thing.Relation> concepts = thing.getRelations(roles);

        Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
            ConceptProto.ThingMethod.Iter.Res res = ConceptProto.ThingMethod.Iter.Res.newBuilder()
                    .setThingGetRelationsIterRes(ConceptProto.Thing.GetRelations.Iter.Res.newBuilder()
                                                         .setRelation(ResponseBuilder.Concept.thing(con))).build();
            return ResponseBuilder.Transaction.Iter.thingMethod(res);
        });

        iterators.startBatchIterating(responses.iterator());
    }

    private void getPlays() {
        Stream<? extends grakn.core.concept.type.RoleType> roleTypes = thing.getPlays();
        Stream<TransactionProto.Transaction.Res> responses = roleTypes.map(con -> {
            ConceptProto.ThingMethod.Iter.Res res = ConceptProto.ThingMethod.Iter.Res.newBuilder()
                    .setThingGetPlaysIterRes(ConceptProto.Thing.GetPlays.Iter.Res.newBuilder()
                                                     .setRoleType(ResponseBuilder.Concept.type(con))).build();
            return ResponseBuilder.Transaction.Iter.thingMethod(res);
        });
        iterators.startBatchIterating(responses.iterator());
    }

    private void setHas(ConceptProto.Thing protoAttribute) {
        grakn.core.concept.thing.Attribute attribute = getThing(protoAttribute).asThing().asAttribute();
        thing.setHas(attribute);
        ConceptProto.ThingMethod.Res response = ConceptProto.ThingMethod.Res.newBuilder()
                .setThingSetHasRes(ConceptProto.Thing.SetHas.Res.newBuilder().build()).build();
        responder.accept(response(response));
    }

    private void unsetHas(ConceptProto.Thing protoAttribute) {
        grakn.core.concept.thing.Attribute attribute = getThing(protoAttribute).asThing().asAttribute();
        thing.asThing().unsetHas(attribute);
        responder.accept(null);
    }

    private class Relation {

        private final grakn.core.concept.thing.Relation relation = ThingRPC.this.thing.asThing().asRelation();

        private void getPlayersByRoleType() {
            Map<? extends grakn.core.concept.type.RoleType, ? extends List<? extends Thing>>
                    playersByRole = relation.getPlayersByRoleType();
            Stream.Builder<TransactionProto.Transaction.Res> responses = Stream.builder();
            for (Map.Entry<? extends grakn.core.concept.type.RoleType,
                    ? extends List<? extends Thing>> players : playersByRole.entrySet()) {
                for (Thing player : players.getValue()) {
                    ConceptProto.ThingMethod.Iter.Res res = ConceptProto.ThingMethod.Iter.Res.newBuilder()
                            .setRelationGetPlayersByRoleTypeIterRes(
                                    ConceptProto.Relation.GetPlayersByRoleType.Iter.Res.newBuilder()
                                            .setRoleType(ResponseBuilder.Concept.type(players.getKey()))
                                            .setPlayer(ResponseBuilder.Concept.thing(player))).build();
                    responses.add(ResponseBuilder.Transaction.Iter.thingMethod(res));
                }
            }

            iterators.startBatchIterating(responses.build().iterator());
        }

        private void getPlayers(List<ConceptProto.Type> protoRoleTypes) {
            final grakn.core.concept.type.RoleType[] roles = protoRoleTypes.stream()
                    .map(ThingRPC.this::getRoleType)
                    .map(ThingRPC::notNull)
                    .toArray(grakn.core.concept.type.RoleType[]::new);
            final Stream<? extends Thing> concepts = relation.getPlayers(roles);

            final Stream<TransactionProto.Transaction.Res> responses = concepts.map(con -> {
                final ConceptProto.ThingMethod.Iter.Res res = ConceptProto.ThingMethod.Iter.Res.newBuilder()
                        .setRelationGetPlayersIterRes(ConceptProto.Relation.GetPlayers.Iter.Res.newBuilder()
                                                              .setThing(ResponseBuilder.Concept.thing(con))).build();
                return ResponseBuilder.Transaction.Iter.thingMethod(res);
            });

            iterators.startBatchIterating(responses.iterator());
        }

        private void addPlayer(ConceptProto.Relation.AddPlayer.Req request) {
            final grakn.core.concept.type.RoleType role = getRoleType(request.getRoleType());
            final Thing player = getThing(request.getPlayer()).asThing();
            relation.addPlayer(role, player);
            responder.accept(null);
        }

        private void removePlayer(ConceptProto.Relation.RemovePlayer.Req request) {
            final grakn.core.concept.type.RoleType role = getRoleType(request.getRoleType());
            final Thing player = getThing(request.getPlayer()).asThing();
            relation.asRelation().removePlayer(role, player);
            responder.accept(null);
        }
    }

    private class Attribute {

        private final grakn.core.concept.thing.Attribute attribute = ThingRPC.this.thing.asThing().asAttribute();

        private void getOwners(ConceptProto.Attribute.GetOwners.Iter.Req request) {
            final Stream<? extends Thing> things;
            switch (request.getFilterCase()) {
                case THINGTYPE:
                    things = attribute.getOwners(getType(request.getThingType()).asThingType());
                    break;
                case FILTER_NOT_SET:
                default:
                    things = attribute.getOwners();
            }

            final Stream<TransactionProto.Transaction.Res> responses = things.map(con -> {
                ConceptProto.ThingMethod.Iter.Res res = ConceptProto.ThingMethod.Iter.Res.newBuilder()
                        .setAttributeGetOwnersIterRes(ConceptProto.Attribute.GetOwners.Iter.Res.newBuilder()
                                                              .setThing(ResponseBuilder.Concept.thing(con))).build();
                return ResponseBuilder.Transaction.Iter.thingMethod(res);
            });

            iterators.startBatchIterating(responses.iterator());
        }
    }
}
