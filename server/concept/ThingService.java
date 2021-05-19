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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.server.TransactionService;
import com.vaticle.typedb.protocol.ConceptProto;
import com.vaticle.typedb.protocol.TransactionProto.Transaction;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static com.vaticle.typedb.core.server.common.RequestReader.byteStringAsUUID;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.Attribute.getOwnersResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.Relation.addPlayerRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.Relation.getPlayersByRoleTypeResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.Relation.getPlayersResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.Relation.getRelatingResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.Relation.removePlayerRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.deleteRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.getHasResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.getPlayingResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.getRelationsResPart;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.getTypeRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.setHasRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.unsetHasRes;

public class ThingService {

    private final TransactionService transactionSvc;
    private final ConceptManager conceptMgr;

    public ThingService(TransactionService transactionSvc, ConceptManager conceptMgr) {
        this.transactionSvc = transactionSvc;
        this.conceptMgr = conceptMgr;
    }

    public void execute(Transaction.Req req) {
        ConceptProto.Thing.Req thingReq = req.getThingReq();
        assert thingReq != null;
        Thing thing = notNull(conceptMgr.getThing(ByteArray.of(thingReq.getIid().toByteArray())));
        UUID reqID = byteStringAsUUID(req.getReqId());
        switch (thingReq.getReqCase()) {
            case THING_DELETE_REQ:
                delete(thing, reqID);
                return;
            case THING_GET_TYPE_REQ:
                getType(thing, reqID);
                return;
            case THING_SET_HAS_REQ:
                setHas(thing, thingReq.getThingSetHasReq().getAttribute(), reqID);
                return;
            case THING_UNSET_HAS_REQ:
                unsetHas(thing, thingReq.getThingUnsetHasReq().getAttribute(), reqID);
                return;
            case THING_GET_HAS_REQ:
                getHas(thing, thingReq.getThingGetHasReq(), reqID);
                return;
            case THING_GET_RELATIONS_REQ:
                getRelations(thing, thingReq.getThingGetRelationsReq().getRoleTypesList(), reqID);
                return;
            case THING_GET_PLAYING_REQ:
                getPlaying(thing, reqID);
                return;
            case RELATION_ADD_PLAYER_REQ:
                addPlayer(thing.asRelation(), thingReq.getRelationAddPlayerReq(), reqID);
                return;
            case RELATION_REMOVE_PLAYER_REQ:
                removePlayer(thing.asRelation(), thingReq.getRelationRemovePlayerReq(), reqID);
                return;
            case RELATION_GET_PLAYERS_REQ:
                getPlayers(thing.asRelation(), thingReq.getRelationGetPlayersReq().getRoleTypesList(), reqID);
                return;
            case RELATION_GET_PLAYERS_BY_ROLE_TYPE_REQ:
                getPlayersByRoleType(thing.asRelation(), reqID);
                return;
            case RELATION_GET_RELATING_REQ:
                getRelating(thing.asRelation(), reqID);
                break;
            case ATTRIBUTE_GET_OWNERS_REQ:
                getOwners(thing.asAttribute(), thingReq.getAttributeGetOwnersReq(), reqID);
                return;
            case REQ_NOT_SET:
            default:
                throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
        }
    }

    private static <T extends Concept> T notNull(@Nullable T concept) {
        if (concept == null) throw TypeDBException.of(MISSING_CONCEPT);
        return concept;
    }

    private Thing getThing(ConceptProto.Thing protoThing) {
        return conceptMgr.getThing(ByteArray.of(protoThing.getIid().toByteArray()));
    }

    private ThingType getThingType(ConceptProto.Type protoType) {
        return conceptMgr.getThingType(protoType.getLabel());
    }

    private RoleType getRoleType(ConceptProto.Type protoRoleType) {
        RelationType relationType = conceptMgr.getRelationType(protoRoleType.getScope());
        if (relationType != null) return relationType.getRelates(protoRoleType.getLabel());
        else return null;
    }

    private void delete(Thing thing, UUID reqID) {
        thing.delete();
        transactionSvc.respond(deleteRes(reqID));
    }

    private void getType(Thing thing, UUID reqID) {
        transactionSvc.respond(getTypeRes(reqID, thing.getType()));
    }

    private void getHas(Thing thing, ConceptProto.Thing.GetHas.Req getHasRequest, UUID reqID) {
        List<ConceptProto.Type> types = getHasRequest.getAttributeTypesList();
        FunctionalIterator<? extends Attribute> attributes = types.isEmpty()
                ? thing.getHas(getHasRequest.getKeysOnly())
                : thing.getHas(types.stream().map(t -> notNull(getThingType(t)).asAttributeType()).toArray(AttributeType[]::new));
        transactionSvc.stream(attributes, reqID, atts -> getHasResPart(reqID, atts));
    }

    private void getRelations(Thing thing, List<ConceptProto.Type> protoRoleTypes, UUID reqID) {
        RoleType[] roleTypes = protoRoleTypes.stream().map(type -> notNull(getRoleType(type))).toArray(RoleType[]::new);
        FunctionalIterator<? extends Relation> concepts = thing.getRelations(roleTypes);
        transactionSvc.stream(concepts, reqID, rels -> getRelationsResPart(reqID, rels));
    }

    private void getPlaying(Thing thing, UUID reqID) {
        FunctionalIterator<? extends RoleType> roleTypes = thing.getPlaying();
        transactionSvc.stream(roleTypes, reqID, rols -> getPlayingResPart(reqID, rols));
    }

    private void setHas(Thing thing, ConceptProto.Thing protoAttribute, UUID reqID) {
        Attribute attribute = getThing(protoAttribute).asAttribute();
        thing.setHas(attribute);
        transactionSvc.respond(setHasRes(reqID));
    }

    private void unsetHas(Thing thing, ConceptProto.Thing protoAttribute, UUID reqID) {
        Attribute attribute = getThing(protoAttribute).asAttribute();
        thing.unsetHas(attribute);
        transactionSvc.respond(unsetHasRes(reqID));
    }

    private void getPlayers(Relation relation, List<ConceptProto.Type> protoRoleTypes, UUID reqID) {
        RoleType[] roleTypes = protoRoleTypes.stream().map(type -> notNull(getRoleType(type))).toArray(RoleType[]::new);
        FunctionalIterator<? extends Thing> players = relation.getPlayers(roleTypes);
        transactionSvc.stream(players, reqID, things -> getPlayersResPart(reqID, things));
    }

    private void getPlayersByRoleType(Relation relation, UUID reqID) {
        // TODO: this should be optimised to actually iterate over role players by role type lazily
        Map<? extends RoleType, ? extends List<? extends Thing>> playersByRole = relation.getPlayersByRoleType();
        Stream.Builder<Pair<RoleType, Thing>> responses = Stream.builder();
        for (Map.Entry<? extends RoleType, ? extends List<? extends Thing>> players : playersByRole.entrySet()) {
            for (Thing player : players.getValue()) {
                responses.add(pair(players.getKey(), player));
            }
        }
        transactionSvc.stream(responses.build().iterator(), reqID, players -> getPlayersByRoleTypeResPart(reqID, players));
    }

    private void getRelating(Relation relation, UUID reqID) {
        transactionSvc.stream(relation.getRelating(), reqID, roleTypes -> getRelatingResPart(reqID, roleTypes));
    }

    private void addPlayer(Relation relation, ConceptProto.Relation.AddPlayer.Req addPlayerReq, UUID reqID) {
        relation.addPlayer(getRoleType(addPlayerReq.getRoleType()), getThing(addPlayerReq.getPlayer()).asThing());
        transactionSvc.respond(addPlayerRes(reqID));
    }

    private void removePlayer(Relation relation, ConceptProto.Relation.RemovePlayer.Req removePlayerReq, UUID reqID) {
        relation.removePlayer(getRoleType(removePlayerReq.getRoleType()), getThing(removePlayerReq.getPlayer()).asThing());
        transactionSvc.respond(removePlayerRes(reqID));
    }

    private void getOwners(Attribute attribute, ConceptProto.Attribute.GetOwners.Req getOwnersReq, UUID reqID) {
        FunctionalIterator<? extends Thing> things;
        switch (getOwnersReq.getFilterCase()) {
            case THING_TYPE:
                things = attribute.getOwners(getThingType(getOwnersReq.getThingType()).asThingType());
                break;
            case FILTER_NOT_SET:
            default:
                things = attribute.getOwners();
        }

        transactionSvc.stream(things, reqID, owners -> getOwnersResPart(reqID, owners));
    }

}
