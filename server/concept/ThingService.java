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
import static grakn.core.server.common.ResponseBuilder.Thing.addPlayerRes;
import static grakn.core.server.common.ResponseBuilder.Thing.deleteRes;
import static grakn.core.server.common.ResponseBuilder.Thing.getHasResPart;
import static grakn.core.server.common.ResponseBuilder.Thing.getOwnersResPart;
import static grakn.core.server.common.ResponseBuilder.Thing.getPlayersByRoleTypeResPart;
import static grakn.core.server.common.ResponseBuilder.Thing.getPlayersResPart;
import static grakn.core.server.common.ResponseBuilder.Thing.getPlaysResPart;
import static grakn.core.server.common.ResponseBuilder.Thing.getRelationsResPart;
import static grakn.core.server.common.ResponseBuilder.Thing.getTypeRes;
import static grakn.core.server.common.ResponseBuilder.Thing.isInferredRes;
import static grakn.core.server.common.ResponseBuilder.Thing.removePlayerRes;
import static grakn.core.server.common.ResponseBuilder.Thing.setHasRes;
import static grakn.core.server.common.ResponseBuilder.Thing.unsetHasRes;

public class ThingService {

    private final TransactionService transactionSrv;
    private final ConceptManager conceptMgr;

    public ThingService(TransactionService transactionSrv, ConceptManager conceptMgr) {
        this.transactionSrv = transactionSrv;
        this.conceptMgr = conceptMgr;
    }

    public void execute(Transaction.Req req) {
        ConceptProto.Thing.Req thingReq = req.getThingReq();
        assert thingReq != null;
        Thing thing = notNull(conceptMgr.getThing(thingReq.getIid().toByteArray()));
        switch (thingReq.getReqCase()) {
            case THING_DELETE_REQ:
                delete(thing, req);
                return;
            case THING_GET_TYPE_REQ:
                getType(thing, req);
                return;
            case THING_IS_INFERRED_REQ:
                isInferred(thing, req);
                return;
            case THING_SET_HAS_REQ:
                setHas(thing, thingReq.getThingSetHasReq().getAttribute(), req);
                return;
            case THING_UNSET_HAS_REQ:
                unsetHas(thing, thingReq.getThingUnsetHasReq().getAttribute(), req);
                return;
            case THING_GET_HAS_REQ:
                getHas(thing, thingReq.getThingGetHasReq(), req);
                return;
            case THING_GET_RELATIONS_REQ:
                getRelations(thing, thingReq.getThingGetRelationsReq().getRoleTypesList(), req);
                return;
            case THING_GET_PLAYS_REQ:
                getPlays(thing, req);
                return;
            case RELATION_ADD_PLAYER_REQ:
                addPlayer(thing.asRelation(), thingReq.getRelationAddPlayerReq(), req);
                return;
            case RELATION_REMOVE_PLAYER_REQ:
                removePlayer(thing.asRelation(), thingReq.getRelationRemovePlayerReq(), req);
                return;
            case RELATION_GET_PLAYERS_REQ:
                getPlayers(thing.asRelation(), thingReq.getRelationGetPlayersReq().getRoleTypesList(), req);
                return;
            case RELATION_GET_PLAYERS_BY_ROLE_TYPE_REQ:
                getPlayersByRoleType(thing.asRelation(), req);
                return;
            case ATTRIBUTE_GET_OWNERS_REQ:
                getOwners(thing.asAttribute(), thingReq.getAttributeGetOwnersReq(), req);
                return;
            case REQ_NOT_SET:
            default:
                throw GraknException.of(UNKNOWN_REQUEST_TYPE);
        }
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

    private void delete(Thing thing, Transaction.Req req) {
        thing.delete();
        transactionSrv.respond(deleteRes(req.getReqId()));
    }

    private void isInferred(Thing thing, Transaction.Req req) {
        String reqID = req.getReqId();
        transactionSrv.respond(isInferredRes(reqID, thing.isInferred()));
    }

    private void getType(Thing thing, Transaction.Req req) {
        transactionSrv.respond(getTypeRes(req.getReqId(), thing.getType()));
    }

    private void getHas(Thing thing, ConceptProto.Thing.GetHas.Req getHasRequest, Transaction.Req req) {
        List<ConceptProto.Type> types = getHasRequest.getAttributeTypesList();
        Stream<? extends Attribute> attributes = types.isEmpty()
                ? thing.getHas(getHasRequest.getKeysOnly())
                : thing.getHas(types.stream().map(t -> notNull(getThingType(t)).asAttributeType()).toArray(AttributeType[]::new));
        transactionSrv.stream(attributes.iterator(), req.getReqId(), atts -> getHasResPart(req.getReqId(), atts));
    }

    private void getRelations(Thing thing, List<ConceptProto.Type> protoRoleTypes, Transaction.Req req) {
        RoleType[] roleTypes = protoRoleTypes.stream().map(type -> notNull(getRoleType(type))).toArray(RoleType[]::new);
        Stream<? extends Relation> concepts = thing.getRelations(roleTypes);
        transactionSrv.stream(concepts.iterator(), req.getReqId(), rels -> getRelationsResPart(req.getReqId(), rels));
    }

    private void getPlays(Thing thing, Transaction.Req req) {
        Stream<? extends RoleType> roleTypes = thing.getPlays();
        transactionSrv.stream(roleTypes.iterator(), req.getReqId(), rols -> getPlaysResPart(req.getReqId(), rols));
    }

    private void setHas(Thing thing, ConceptProto.Thing protoAttribute, Transaction.Req req) {
        Attribute attribute = getThing(protoAttribute).asAttribute();
        thing.setHas(attribute);
        transactionSrv.respond(setHasRes(req.getReqId()));
    }

    private void unsetHas(Thing thing, ConceptProto.Thing protoAttribute, Transaction.Req req) {
        Attribute attribute = getThing(protoAttribute).asAttribute();
        thing.unsetHas(attribute);
        transactionSrv.respond(unsetHasRes(req.getReqId()));
    }

    private void getPlayersByRoleType(Relation relation, Transaction.Req req) {
        // TODO: this should be optimised to actually iterate over role players by role type lazily
        Map<? extends RoleType, ? extends List<? extends Thing>> playersByRole = relation.getPlayersByRoleType();
        Stream.Builder<Pair<RoleType, Thing>> responses = Stream.builder();
        for (Map.Entry<? extends RoleType, ? extends List<? extends Thing>> players : playersByRole.entrySet()) {
            for (Thing player : players.getValue()) {
                responses.add(pair(players.getKey(), player));
            }
        }
        transactionSrv.stream(responses.build().iterator(), req.getReqId(), rps -> getPlayersByRoleTypeResPart(req.getReqId(), rps));
    }

    private void getPlayers(Relation relation, List<ConceptProto.Type> protoRoleTypes, Transaction.Req req) {
        RoleType[] roleTypes = protoRoleTypes.stream().map(type -> notNull(getRoleType(type))).toArray(RoleType[]::new);
        Stream<? extends Thing> things = relation.getPlayers(roleTypes);
        transactionSrv.stream(things.iterator(), req.getReqId(), cons -> getPlayersResPart(req.getReqId(), cons));
    }

    private void addPlayer(Relation relation, ConceptProto.Relation.AddPlayer.Req addPlayerReq, Transaction.Req req) {
        relation.addPlayer(getRoleType(addPlayerReq.getRoleType()), getThing(addPlayerReq.getPlayer()).asThing());
        transactionSrv.respond(addPlayerRes(req.getReqId()));
    }

    private void removePlayer(Relation relation, ConceptProto.Relation.RemovePlayer.Req removePlayerReq, Transaction.Req req) {
        relation.removePlayer(getRoleType(removePlayerReq.getRoleType()), getThing(removePlayerReq.getPlayer()).asThing());
        transactionSrv.respond(removePlayerRes(req.getReqId()));
    }

    private void getOwners(Attribute attribute, ConceptProto.Attribute.GetOwners.Req getOwnersReq, Transaction.Req req) {
        Stream<? extends Thing> things;
        switch (getOwnersReq.getFilterCase()) {
            case THING_TYPE:
                things = attribute.getOwners(getThingType(getOwnersReq.getThingType()).asThingType());
                break;
            case FILTER_NOT_SET:
            default:
                things = attribute.getOwners();
        }

        transactionSrv.stream(things.iterator(), req.getReqId(), owners -> getOwnersResPart(req.getReqId(), owners));
    }

}
