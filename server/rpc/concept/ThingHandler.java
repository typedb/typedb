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

import grakn.core.common.exception.GraknException;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.RoleType;
import grakn.core.server.rpc.TransactionRPC;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto.Transaction;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.server.rpc.util.ResponseBuilder.Concept.thing;
import static grakn.core.server.rpc.util.ResponseBuilder.Concept.type;

public class ThingHandler {

    private final TransactionRPC transactionRPC;
    private final ConceptManager conceptManager;

    public ThingHandler(final TransactionRPC transactionRPC, final ConceptManager conceptManager) {
        this.transactionRPC = transactionRPC;
        this.conceptManager = conceptManager;
    }

    public void handleRequest(final Transaction.Req request) {
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
                throw new GraknException(UNKNOWN_REQUEST_TYPE);
        }
    }

    private static Transaction.Res response(final Transaction.Req request, final ConceptProto.Thing.Res.Builder response) {
        return Transaction.Res.newBuilder().setId(request.getId()).setThingRes(response).build();
    }

    private static <T extends Concept> T notNull(@Nullable final T concept) {
        if (concept == null) throw new GraknException(MISSING_CONCEPT);
        return concept;
    }

    private Thing getThing(final ConceptProto.Thing protoThing) {
        return conceptManager.getThing(protoThing.getIid().toByteArray());
    }

    private grakn.core.concept.type.Type getType(final ConceptProto.Type protoType) {
        return conceptManager.getType(protoType.getLabel());
    }

    private grakn.core.concept.type.RoleType getRoleType(final ConceptProto.Type protoRole) {
        return conceptManager.getRelationType(protoRole.getScope()).getRelates(protoRole.getLabel());
    }

    private void delete(final Transaction.Req request, final Thing thing) {
        thing.delete();
        transactionRPC.respond(response(request, ConceptProto.Thing.Res.newBuilder().setThingDeleteRes(ConceptProto.Thing.Delete.Res.getDefaultInstance())));
    }

    private void isInferred(final Transaction.Req request, final Thing thing) {
        final ConceptProto.Thing.Res.Builder response = ConceptProto.Thing.Res.newBuilder()
                .setThingIsInferredRes(ConceptProto.Thing.IsInferred.Res.newBuilder().setInferred(thing.isInferred()));
        transactionRPC.respond(response(request, response));
    }

    private void getType(final Transaction.Req request, final Thing thing) {
        final ConceptProto.Thing.Res.Builder response = ConceptProto.Thing.Res.newBuilder()
                .setThingGetTypeRes(ConceptProto.Thing.GetType.Res.newBuilder().setThingType(type(thing.getType())));
        transactionRPC.respond(response(request, response));
    }

    private void getHas(final Transaction.Req request, final Thing thing, final ConceptProto.Thing.GetHas.Req req) {
        final List<ConceptProto.Type> protoTypes = req.getAttributeTypesList();
        final Stream<? extends Attribute> attributes;

        if (protoTypes.isEmpty()) {
            attributes = thing.getHas(req.getKeysOnly());
        } else {
            final AttributeType[] attributeTypes = protoTypes.stream()
                    .map(type -> notNull(getType(type)).asAttributeType())
                    .toArray(AttributeType[]::new);
            attributes = thing.getHas(attributeTypes);
        }

        final Stream<Transaction.Res> responses = attributes.map(con -> response(request, ConceptProto.Thing.Res.newBuilder()
                .setThingGetHasRes(ConceptProto.Thing.GetHas.Res.newBuilder().setAttribute(thing(con)))));
        transactionRPC.respond(request, responses.iterator());
    }

    private void getRelations(final Transaction.Req request, final Thing thing, final List<ConceptProto.Type> protoRoleTypes) {
        final RoleType[] roleTypes = protoRoleTypes.stream()
                .map(type -> notNull(getRoleType(type)))
                .toArray(RoleType[]::new);
        final Stream<? extends Relation> concepts = thing.getRelations(roleTypes);

        final Stream<Transaction.Res> responses = concepts.map(con -> response(request, ConceptProto.Thing.Res.newBuilder()
                .setThingGetRelationsRes(ConceptProto.Thing.GetRelations.Res.newBuilder().setRelation(thing(con)))));
        transactionRPC.respond(request, responses.iterator());
    }

    private void getPlays(final Transaction.Req request, final Thing thing) {
        final Stream<? extends RoleType> roleTypes = thing.getPlays();
        final Stream<Transaction.Res> responses = roleTypes.map(con -> response(request, ConceptProto.Thing.Res.newBuilder()
                .setThingGetPlaysRes(ConceptProto.Thing.GetPlays.Res.newBuilder().setRoleType(type(con)))));
        transactionRPC.respond(request, responses.iterator());
    }

    private void setHas(final Transaction.Req request, final Thing thing, final ConceptProto.Thing protoAttribute) {
        final Attribute attribute = getThing(protoAttribute).asAttribute();
        thing.setHas(attribute);
        final ConceptProto.Thing.Res.Builder response = ConceptProto.Thing.Res.newBuilder()
                .setThingSetHasRes(ConceptProto.Thing.SetHas.Res.getDefaultInstance());
        transactionRPC.respond(response(request, response));
    }

    private void unsetHas(final Transaction.Req request, final Thing thing, final ConceptProto.Thing protoAttribute) {
        final Attribute attribute = getThing(protoAttribute).asAttribute();
        thing.unsetHas(attribute);
        final ConceptProto.Thing.Res.Builder response = ConceptProto.Thing.Res.newBuilder()
                .setThingUnsetHasRes(ConceptProto.Thing.UnsetHas.Res.getDefaultInstance());
        transactionRPC.respond(response(request, response));
    }

    private void getPlayersByRoleType(final Transaction.Req request, final Relation relation) {
        final Map<? extends RoleType, ? extends List<? extends Thing>> playersByRole = relation.getPlayersByRoleType();
        final Stream.Builder<Transaction.Res> responses = Stream.builder();
        for (Map.Entry<? extends RoleType, ? extends List<? extends Thing>> players : playersByRole.entrySet()) {
            for (Thing player : players.getValue()) {
                final ConceptProto.Thing.Res.Builder res = ConceptProto.Thing.Res.newBuilder()
                        .setRelationGetPlayersByRoleTypeRes(ConceptProto.Relation.GetPlayersByRoleType.Res.newBuilder()
                                .setRoleType(type(players.getKey()))
                                .setPlayer(thing(player)));
                responses.add(response(request, res));
            }
        }
        transactionRPC.respond(request, responses.build().iterator());
    }

    private void getPlayers(final Transaction.Req request, final Relation relation, final List<ConceptProto.Type> protoRoleTypes) {
        final RoleType[] roleTypes = protoRoleTypes.stream()
                .map(type -> notNull(getRoleType(type)))
                .toArray(RoleType[]::new);
        final Stream<? extends Thing> things = relation.getPlayers(roleTypes);

        final Stream<Transaction.Res> responses = things.map(con -> response(request, ConceptProto.Thing.Res.newBuilder()
                .setRelationGetPlayersRes(ConceptProto.Relation.GetPlayers.Res.newBuilder().setThing(thing(con)))));
        transactionRPC.respond(request, responses.iterator());
    }

    private void addPlayer(final Transaction.Req request, final Relation relation, final ConceptProto.Relation.AddPlayer.Req addPlayerReq) {
        final RoleType role = getRoleType(addPlayerReq.getRoleType());
        final Thing player = getThing(addPlayerReq.getPlayer()).asThing();
        relation.addPlayer(role, player);
        final ConceptProto.Thing.Res.Builder response = ConceptProto.Thing.Res.newBuilder()
                .setRelationAddPlayerRes(ConceptProto.Relation.AddPlayer.Res.getDefaultInstance());
        transactionRPC.respond(response(request, response));
    }

    private void removePlayer(final Transaction.Req request, final Relation relation, final ConceptProto.Relation.RemovePlayer.Req removePlayerReq) {
        final RoleType role = getRoleType(removePlayerReq.getRoleType());
        final Thing player = getThing(removePlayerReq.getPlayer()).asThing();
        relation.removePlayer(role, player);
        final ConceptProto.Thing.Res.Builder response = ConceptProto.Thing.Res.newBuilder()
                .setRelationRemovePlayerRes(ConceptProto.Relation.RemovePlayer.Res.getDefaultInstance());
        transactionRPC.respond(response(request, response));
    }

    private void getOwners(final Transaction.Req request, final Attribute attribute, final ConceptProto.Attribute.GetOwners.Req getOwnersReq) {
        final Stream<? extends Thing> things;
        switch (getOwnersReq.getFilterCase()) {
            case THING_TYPE:
                things = attribute.getOwners(getType(getOwnersReq.getThingType()).asThingType());
                break;
            case FILTER_NOT_SET:
            default:
                things = attribute.getOwners();
        }

        final Stream<Transaction.Res> responses = things.map(con -> response(request, ConceptProto.Thing.Res.newBuilder()
                .setAttributeGetOwnersRes(ConceptProto.Attribute.GetOwners.Res.newBuilder().setThing(thing(con)))));
        transactionRPC.respond(request, responses.iterator());
    }
}
