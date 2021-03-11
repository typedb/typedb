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
import grakn.core.server.TransactionService;
import grakn.core.server.common.ResponseBuilder;
import grakn.protocol.ConceptProto;
import grakn.protocol.TransactionProto.Transaction;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static grakn.common.collection.Collections.pair;
import static grakn.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static grakn.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static grakn.core.server.common.ResponseBuilder.Concept.thing;
import static grakn.core.server.common.ResponseBuilder.Concept.type;
import static java.util.stream.Collectors.toList;

public class ThingService {

    private final TransactionService transactionSrv;
    private final ConceptManager conceptMgr;

    public ThingService(TransactionService transactionSrv, ConceptManager conceptMgr) {
        this.transactionSrv = transactionSrv;
        this.conceptMgr = conceptMgr;
    }

    public void execute(Transaction.Req request) {
        ConceptProto.Thing.Req thingReq = request.getThingReq();
        assert thingReq != null;
        Thing thing = notNull(conceptMgr.getThing(thingReq.getIid().toByteArray()));
        switch (thingReq.getReqCase()) {
            case THING_DELETE_REQ:
                delete(thing, request);
                return;
            case THING_GET_TYPE_REQ:
                getType(thing, request);
                return;
            case THING_IS_INFERRED_REQ:
                isInferred(thing, request);
                return;
            case THING_SET_HAS_REQ:
                setHas(thing, thingReq.getThingSetHasReq().getAttribute(), request);
                return;
            case THING_UNSET_HAS_REQ:
                unsetHas(thing, thingReq.getThingUnsetHasReq().getAttribute(), request);
                return;
            case THING_GET_HAS_REQ:
                getHas(thing, thingReq.getThingGetHasReq(), request);
                return;
            case THING_GET_RELATIONS_REQ:
                getRelations(thing, thingReq.getThingGetRelationsReq().getRoleTypesList(), request);
                return;
            case THING_GET_PLAYS_REQ:
                getPlays(thing, request);
                return;
            case RELATION_ADD_PLAYER_REQ:
                addPlayer(thing.asRelation(), thingReq.getRelationAddPlayerReq(), request);
                return;
            case RELATION_REMOVE_PLAYER_REQ:
                removePlayer(thing.asRelation(), thingReq.getRelationRemovePlayerReq(), request);
                return;
            case RELATION_GET_PLAYERS_REQ:
                getPlayers(thing.asRelation(), thingReq.getRelationGetPlayersReq().getRoleTypesList(), request);
                return;
            case RELATION_GET_PLAYERS_BY_ROLE_TYPE_REQ:
                getPlayersByRoleType(thing.asRelation(), request);
                return;
            case ATTRIBUTE_GET_OWNERS_REQ:
                getOwners(thing.asAttribute(), thingReq.getAttributeGetOwnersReq(), request);
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
        return conceptMgr.getThing(protoThing.getIid().toByteArray());
    }

    private ThingType getThingType(ConceptProto.Type protoType) {
        return conceptMgr.getThingType(protoType.getLabel());
    }

    private RoleType getRoleType(ConceptProto.Type protoRoleType) {
        RelationType relationType = conceptMgr.getRelationType(protoRoleType.getScope());
        if (relationType != null) return relationType.getRelates(protoRoleType.getLabel());
        else return null;
    }

    private void delete(Thing thing, Transaction.Req request) {
        thing.delete();
        transactionSrv.respond(response(request, ConceptProto.Thing.Res.newBuilder().setThingDeleteRes(
                ConceptProto.Thing.Delete.Res.getDefaultInstance())
        ));
    }

    private void isInferred(Thing thing, Transaction.Req request) {
        transactionSrv.respond(response(request, ConceptProto.Thing.Res.newBuilder().setThingIsInferredRes(
                ConceptProto.Thing.IsInferred.Res.newBuilder().setInferred(thing.isInferred())
        )));
    }

    private void getType(Thing thing, Transaction.Req request) {
        transactionSrv.respond(response(request, ConceptProto.Thing.Res.newBuilder().setThingGetTypeRes(
                ConceptProto.Thing.GetType.Res.newBuilder().setThingType(type(thing.getType()))
        )));
    }

    private void getHas(Thing thing, ConceptProto.Thing.GetHas.Req getHasRequest, Transaction.Req request) {
        List<ConceptProto.Type> types = getHasRequest.getAttributeTypesList();
        Stream<? extends Attribute> attributes = types.isEmpty()
                ? thing.getHas(getHasRequest.getKeysOnly())
                : thing.getHas(types.stream().map(t -> notNull(getThingType(t)).asAttributeType()).toArray(AttributeType[]::new));

        transactionSrv.stream(attributes.iterator(), request.getId(), cons -> response(
                request, ConceptProto.Thing.Res.newBuilder().setThingGetHasRes(
                        ConceptProto.Thing.GetHas.Res.newBuilder().addAllAttributes(
                                cons.stream().map(ResponseBuilder.Concept::thing).collect(toList())))
        ));
    }

    private void getRelations(Thing thing, List<ConceptProto.Type> protoRoleTypes, Transaction.Req request) {
        RoleType[] roleTypes = protoRoleTypes.stream().map(type -> notNull(getRoleType(type))).toArray(RoleType[]::new);
        Stream<? extends Relation> concepts = thing.getRelations(roleTypes);
        transactionSrv.stream(concepts.iterator(), request.getId(), cons -> response(
                request, ConceptProto.Thing.Res.newBuilder().setThingGetRelationsRes(
                        ConceptProto.Thing.GetRelations.Res.newBuilder().addAllRelations(
                                cons.stream().map(ResponseBuilder.Concept::thing).collect(toList())))
        ));
    }

    private void getPlays(Thing thing, Transaction.Req request) {
        Stream<? extends RoleType> roleTypes = thing.getPlays();
        transactionSrv.stream(roleTypes.iterator(), request.getId(), cons -> response(
                request, ConceptProto.Thing.Res.newBuilder().setThingGetPlaysRes(
                        ConceptProto.Thing.GetPlays.Res.newBuilder().addAllRoleTypes(
                                cons.stream().map(ResponseBuilder.Concept::type).collect(toList())))
        ));
    }

    private void setHas(Thing thing, ConceptProto.Thing protoAttribute, Transaction.Req request) {
        Attribute attribute = getThing(protoAttribute).asAttribute();
        thing.setHas(attribute);
        transactionSrv.respond(response(request, ConceptProto.Thing.Res.newBuilder().setThingSetHasRes(
                ConceptProto.Thing.SetHas.Res.getDefaultInstance()
        )));
    }

    private void unsetHas(Thing thing, ConceptProto.Thing protoAttribute, Transaction.Req request) {
        Attribute attribute = getThing(protoAttribute).asAttribute();
        thing.unsetHas(attribute);
        transactionSrv.respond(response(request, ConceptProto.Thing.Res.newBuilder().setThingUnsetHasRes(
                ConceptProto.Thing.UnsetHas.Res.getDefaultInstance()
        )));
    }

    private void getPlayersByRoleType(Relation relation, Transaction.Req request) {
        Map<? extends RoleType, ? extends List<? extends Thing>> playersByRole = relation.getPlayersByRoleType();
        Stream.Builder<Pair<RoleType, Thing>> responses = Stream.builder();
        for (Map.Entry<? extends RoleType, ? extends List<? extends Thing>> players : playersByRole.entrySet()) {
            for (Thing player : players.getValue()) {
                responses.add(pair(players.getKey(), player));
            }
        }
        transactionSrv.stream(responses.build().iterator(), request.getId(), cons -> response(
                request, ConceptProto.Thing.Res.newBuilder().setRelationGetPlayersByRoleTypeRes(
                        ConceptProto.Relation.GetPlayersByRoleType.Res.newBuilder().addAllRoleTypesWithPlayers(
                                cons.stream().map(con -> ConceptProto.Relation.GetPlayersByRoleType.RoleTypeWithPlayer.newBuilder()
                                        .setRoleType(type(con.first())).setPlayer(thing(con.second())).build()).collect(toList())))
        ));
    }

    private void getPlayers(Relation relation, List<ConceptProto.Type> protoRoleTypes, Transaction.Req request) {
        RoleType[] roleTypes = protoRoleTypes.stream().map(type -> notNull(getRoleType(type))).toArray(RoleType[]::new);
        Stream<? extends Thing> things = relation.getPlayers(roleTypes);
        transactionSrv.stream(things.iterator(), request.getId(), cons -> response(
                request, ConceptProto.Thing.Res.newBuilder().setRelationGetPlayersRes(
                        ConceptProto.Relation.GetPlayers.Res.newBuilder().addAllThings(
                                cons.stream().map(ResponseBuilder.Concept::thing).collect(toList())))
        ));
    }

    private void addPlayer(Relation relation, ConceptProto.Relation.AddPlayer.Req addPlayerReq, Transaction.Req request) {
        RoleType roleType = getRoleType(addPlayerReq.getRoleType());
        Thing player = getThing(addPlayerReq.getPlayer()).asThing();
        relation.addPlayer(roleType, player);
        transactionSrv.respond(response(request, ConceptProto.Thing.Res.newBuilder().setRelationAddPlayerRes(
                ConceptProto.Relation.AddPlayer.Res.getDefaultInstance()
        )));
    }

    private void removePlayer(Relation relation, ConceptProto.Relation.RemovePlayer.Req removePlayerReq,
                              Transaction.Req request) {
        RoleType roleType = getRoleType(removePlayerReq.getRoleType());
        Thing player = getThing(removePlayerReq.getPlayer()).asThing();
        relation.removePlayer(roleType, player);
        transactionSrv.respond(response(request, ConceptProto.Thing.Res.newBuilder().setRelationRemovePlayerRes(
                ConceptProto.Relation.RemovePlayer.Res.getDefaultInstance()
        )));
    }

    private void getOwners(Attribute attribute, ConceptProto.Attribute.GetOwners.Req getOwnersReq,
                           Transaction.Req request) {
        Stream<? extends Thing> things;
        switch (getOwnersReq.getFilterCase()) {
            case THING_TYPE:
                things = attribute.getOwners(getThingType(getOwnersReq.getThingType()).asThingType());
                break;
            case FILTER_NOT_SET:
            default:
                things = attribute.getOwners();
        }

        transactionSrv.stream(things.iterator(), request.getId(), cons -> response(
                request, ConceptProto.Thing.Res.newBuilder().setAttributeGetOwnersRes(
                        ConceptProto.Attribute.GetOwners.Res.newBuilder().addAllThings(
                                cons.stream().map(ResponseBuilder.Concept::thing).collect(toList())))
        ));
    }
}
