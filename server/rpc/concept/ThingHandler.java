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

import grakn.common.collection.Pair;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.ThingType;
import grakn.core.server.rpc.TransactionRPC;
import grakn.core.server.rpc.util.ResponseBuilder;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto.Transaction;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.pair;
import static grakn.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.server.rpc.util.ResponseBuilder.Concept.thing;
import static grakn.core.server.rpc.util.ResponseBuilder.Concept.type;

public class ThingHandler {

    private final TransactionRPC transactionRPC;
    private final ConceptManager conceptManager;

    public ThingHandler(TransactionRPC transactionRPC, ConceptManager conceptManager) {
        this.transactionRPC = transactionRPC;
        this.conceptManager = conceptManager;
    }

    public void handleRequest(Transaction.Req request) {
        final ConceptProto.Thing.Req thingReq = request.getThingReq();
        assert thingReq != null;
        final Thing thing = notNull(conceptManager.getThing(thingReq.getIid().toByteArray()));
        switch (thingReq.getReqCase()) {
            case THING_DELETE_REQ:
                delete(request, thing);
                return;
            case THING_GET_TYPE_REQ:
                getType(request, thing);
                return;
            case THING_IS_INFERRED_REQ:
                isInferred(request, thing);
                return;
            case THING_SET_HAS_REQ:
                setHas(request, thing, thingReq.getThingSetHasReq().getAttribute());
                return;
            case THING_UNSET_HAS_REQ:
                unsetHas(request, thing, thingReq.getThingUnsetHasReq().getAttribute());
                return;
            case THING_GET_HAS_REQ:
                getHas(request, thing, thingReq.getThingGetHasReq());
                return;
            case THING_GET_RELATIONS_REQ:
                getRelations(request, thing, thingReq.getThingGetRelationsReq().getRoleTypesList());
                return;
            case THING_GET_PLAYS_REQ:
                getPlays(request, thing);
                return;
            case RELATION_ADD_PLAYER_REQ:
                addPlayer(request, thing.asRelation(), thingReq.getRelationAddPlayerReq());
                return;
            case RELATION_REMOVE_PLAYER_REQ:
                removePlayer(request, thing.asRelation(), thingReq.getRelationRemovePlayerReq());
                return;
            case RELATION_GET_PLAYERS_REQ:
                getPlayers(request, thing.asRelation(), thingReq.getRelationGetPlayersReq().getRoleTypesList());
                return;
            case RELATION_GET_PLAYERS_BY_ROLE_TYPE_REQ:
                getPlayersByRoleType(request, thing.asRelation());
                return;
            case ATTRIBUTE_GET_OWNERS_REQ:
                getOwners(request, thing.asAttribute(), thingReq.getAttributeGetOwnersReq());
                return;
            case REQ_NOT_SET:
            default:
                throw GraknException.of(UNKNOWN_REQUEST_TYPE);
        }
    }

    private static Transaction.Res response(Transaction.Req request, ConceptProto.Thing.Res.Builder response) {
        return Transaction.Res.newBuilder().setId(request.getId()).setThingRes(response).build();
    }

    private static <T extends Concept> T notNull(@Nullable T concept) {
        if (concept == null) throw GraknException.of(MISSING_CONCEPT);
        return concept;
    }

    private Thing getThing(ConceptProto.Thing protoThing) {
        return conceptManager.getThing(protoThing.getIid().toByteArray());
    }

    private ThingType getThingType(ConceptProto.Type protoType) {
        return conceptManager.getThingType(protoType.getLabel());
    }

    private RoleType getRoleType(ConceptProto.Type protoRoleType) {
        RelationType relationType = conceptManager.getRelationType(protoRoleType.getScope());
        if (relationType != null) return relationType.getRelates(protoRoleType.getLabel());
        else return null;
    }

    private void delete(Transaction.Req request, Thing thing) {
        thing.delete();
        transactionRPC.respond(response(request, ConceptProto.Thing.Res.newBuilder().setThingDeleteRes(
                ConceptProto.Thing.Delete.Res.getDefaultInstance())
        ));
    }

    private void isInferred(Transaction.Req request, Thing thing) {
        transactionRPC.respond(response(request, ConceptProto.Thing.Res.newBuilder().setThingIsInferredRes(
                ConceptProto.Thing.IsInferred.Res.newBuilder().setInferred(thing.isInferred())
        )));
    }

    private void getType(Transaction.Req request, Thing thing) {
        transactionRPC.respond(response(request, ConceptProto.Thing.Res.newBuilder().setThingGetTypeRes(
                ConceptProto.Thing.GetType.Res.newBuilder().setThingType(type(thing.getType()))
        )));
    }

    private void getHas(Transaction.Req request, Thing thing, ConceptProto.Thing.GetHas.Req req) {
        final List<ConceptProto.Type> protoTypes = req.getAttributeTypesList();
        final Stream<? extends Attribute> attributes;

        if (protoTypes.isEmpty()) {
            attributes = thing.getHas(req.getKeysOnly());
        } else {
            final AttributeType[] attributeTypes = protoTypes.stream()
                    .map(type -> notNull(getThingType(type)).asAttributeType())
                    .toArray(AttributeType[]::new);
            attributes = thing.getHas(attributeTypes);
        }

        transactionRPC.respond(
                request, attributes.iterator(),
                cons -> response(request, ConceptProto.Thing.Res.newBuilder().setThingGetHasRes(
                        ConceptProto.Thing.GetHas.Res.newBuilder().addAllAttributes(
                                cons.stream().map(ResponseBuilder.Concept::thing).collect(Collectors.toList()))))
        );
    }

    private void getRelations(Transaction.Req request, Thing thing, List<ConceptProto.Type> protoRoleTypes) {
        final RoleType[] roleTypes = protoRoleTypes.stream()
                .map(type -> notNull(getRoleType(type)))
                .toArray(RoleType[]::new);
        final Stream<? extends Relation> concepts = thing.getRelations(roleTypes);
        transactionRPC.respond(
                request, concepts.iterator(),
                cons -> response(request, ConceptProto.Thing.Res.newBuilder().setThingGetRelationsRes(
                        ConceptProto.Thing.GetRelations.Res.newBuilder().addAllRelations(
                                cons.stream().map(ResponseBuilder.Concept::thing).collect(Collectors.toList()))))
        );
    }

    private void getPlays(Transaction.Req request, Thing thing) {
        final Stream<? extends RoleType> roleTypes = thing.getPlays();
        transactionRPC.respond(
                request, roleTypes.iterator(),
                cons -> response(request, ConceptProto.Thing.Res.newBuilder().setThingGetPlaysRes(
                        ConceptProto.Thing.GetPlays.Res.newBuilder().addAllRoleTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(Collectors.toList()))))
        );
    }

    private void setHas(Transaction.Req request, Thing thing, ConceptProto.Thing protoAttribute) {
        final Attribute attribute = getThing(protoAttribute).asAttribute();
        thing.setHas(attribute);
        transactionRPC.respond(response(request, ConceptProto.Thing.Res.newBuilder().setThingSetHasRes(
                ConceptProto.Thing.SetHas.Res.getDefaultInstance()
        )));
    }

    private void unsetHas(Transaction.Req request, Thing thing, ConceptProto.Thing protoAttribute) {
        final Attribute attribute = getThing(protoAttribute).asAttribute();
        thing.unsetHas(attribute);
        transactionRPC.respond(response(request, ConceptProto.Thing.Res.newBuilder().setThingUnsetHasRes(
                ConceptProto.Thing.UnsetHas.Res.getDefaultInstance()
        )));
    }

    private void getPlayersByRoleType(Transaction.Req request, Relation relation) {
        final Map<? extends RoleType, ? extends List<? extends Thing>> playersByRole = relation.getPlayersByRoleType();
        final Stream.Builder<Pair<RoleType, Thing>> responses = Stream.builder();
        for (Map.Entry<? extends RoleType, ? extends List<? extends Thing>> players : playersByRole.entrySet()) {
            for (Thing player : players.getValue()) {
                responses.add(pair(players.getKey(), player));
            }
        }
        transactionRPC.respond(
                request, responses.build().iterator(),
                cons -> response(request, ConceptProto.Thing.Res.newBuilder().setRelationGetPlayersByRoleTypeRes(
                        ConceptProto.Relation.GetPlayersByRoleType.Res.newBuilder().addAllRoleTypesWithPlayers(
                                cons.stream().map(con -> ConceptProto.Relation.GetPlayersByRoleType.RoleTypeWithPlayer.newBuilder()
                                        .setRoleType(type(con.first()))
                                        .setPlayer(thing(con.second())).build()).collect(Collectors.toList()))))
        );
    }

    private void getPlayers(Transaction.Req request, Relation relation, List<ConceptProto.Type> protoRoleTypes) {
        final RoleType[] roleTypes = protoRoleTypes.stream()
                .map(type -> notNull(getRoleType(type)))
                .toArray(RoleType[]::new);
        final Stream<? extends Thing> things = relation.getPlayers(roleTypes);

        transactionRPC.respond(
                request, things.iterator(),
                cons -> response(request, ConceptProto.Thing.Res.newBuilder().setRelationGetPlayersRes(
                        ConceptProto.Relation.GetPlayers.Res.newBuilder().addAllThings(
                                cons.stream().map(ResponseBuilder.Concept::thing).collect(Collectors.toList()))))
        );
    }

    private void addPlayer(Transaction.Req request, Relation relation, ConceptProto.Relation.AddPlayer.Req addPlayerReq) {
        final RoleType roleType = getRoleType(addPlayerReq.getRoleType());
        final Thing player = getThing(addPlayerReq.getPlayer()).asThing();
        relation.addPlayer(roleType, player);
        transactionRPC.respond(response(request, ConceptProto.Thing.Res.newBuilder().setRelationAddPlayerRes(
                ConceptProto.Relation.AddPlayer.Res.getDefaultInstance()
        )));
    }

    private void removePlayer(Transaction.Req request, Relation relation, ConceptProto.Relation.RemovePlayer.Req removePlayerReq) {
        final RoleType roleType = getRoleType(removePlayerReq.getRoleType());
        final Thing player = getThing(removePlayerReq.getPlayer()).asThing();
        relation.removePlayer(roleType, player);
        transactionRPC.respond(response(request, ConceptProto.Thing.Res.newBuilder().setRelationRemovePlayerRes(
                ConceptProto.Relation.RemovePlayer.Res.getDefaultInstance()
        )));
    }

    private void getOwners(Transaction.Req request, Attribute attribute, ConceptProto.Attribute.GetOwners.Req getOwnersReq) {
        final Stream<? extends Thing> things;
        switch (getOwnersReq.getFilterCase()) {
            case THING_TYPE:
                things = attribute.getOwners(getThingType(getOwnersReq.getThingType()).asThingType());
                break;
            case FILTER_NOT_SET:
            default:
                things = attribute.getOwners();
        }

        transactionRPC.respond(
                request, things.iterator(),
                cons -> response(request, ConceptProto.Thing.Res.newBuilder().setAttributeGetOwnersRes(
                        ConceptProto.Attribute.GetOwners.Res.newBuilder().addAllThings(
                                cons.stream().map(ResponseBuilder.Concept::thing).collect(Collectors.toList()))))
        );
    }
}
