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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.ConceptManager;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Entity;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.AttributeType;
import com.vaticle.typedb.core.concept.type.EntityType;
import com.vaticle.typedb.core.concept.type.RelationType;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.ThingType;
import com.vaticle.typedb.core.server.TransactionService;
import com.vaticle.typedb.protocol.ConceptProto;
import com.vaticle.typedb.protocol.TransactionProto.Transaction;
import com.vaticle.typeql.lang.common.TypeQLToken;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.BAD_VALUE_TYPE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.MISSING_CONCEPT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Server.UNKNOWN_REQUEST_TYPE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
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
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.setHasRes;
import static com.vaticle.typedb.core.server.common.ResponseBuilder.Thing.unsetHasRes;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.ATTRIBUTE_GET_OWNERS_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.RELATION_ADD_PLAYER_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.RELATION_GET_PLAYERS_BY_ROLE_TYPE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.RELATION_GET_PLAYERS_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.RELATION_GET_RELATING_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.RELATION_REMOVE_PLAYER_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.REQ_NOT_SET;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.THING_DELETE_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.THING_GET_HAS_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.THING_GET_PLAYING_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.THING_GET_RELATIONS_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.THING_SET_HAS_REQ;
import static com.vaticle.typedb.protocol.ConceptProto.Thing.Req.ReqCase.THING_UNSET_HAS_REQ;

public class ThingService {

    private final TransactionService transactionSvc;
    private final ConceptManager conceptMgr;

    private final EnumMap<ConceptProto.Thing.Req.ReqCase, BiConsumer<ConceptProto.Thing.Req, UUID>> handlers;

    public ThingService(TransactionService transactionSvc, ConceptManager conceptMgr) {
        this.transactionSvc = transactionSvc;
        this.conceptMgr = conceptMgr;

        handlers = new EnumMap<>(ConceptProto.Thing.Req.ReqCase.class);
        handlers.put(THING_DELETE_REQ, this::delete);
        handlers.put(THING_GET_HAS_REQ, this::getHas);
        handlers.put(THING_SET_HAS_REQ, this::setHas);
        handlers.put(THING_UNSET_HAS_REQ, this::unsetHas);
        handlers.put(THING_GET_RELATIONS_REQ, this::getRelations);
        handlers.put(THING_GET_PLAYING_REQ, this::getPlaying);
        handlers.put(RELATION_ADD_PLAYER_REQ, this::relationAddPlayer);
        handlers.put(RELATION_REMOVE_PLAYER_REQ, this::relationRemovePlayer);
        handlers.put(RELATION_GET_PLAYERS_REQ, this::relationGetPlayers);
        handlers.put(RELATION_GET_PLAYERS_BY_ROLE_TYPE_REQ, this::relationGetPlayersByRoleType);
        handlers.put(RELATION_GET_RELATING_REQ, this::relationGetRelating);
        handlers.put(ATTRIBUTE_GET_OWNERS_REQ, this::attributeGetOwners);
        handlers.put(REQ_NOT_SET, this::requestNotSet);
        assert handlers.size() == ConceptProto.Thing.Req.ReqCase.class.getEnumConstants().length;
    }

    public void execute(Transaction.Req req) {
        ConceptProto.Thing.Req thingReq = req.getThingReq();
        UUID reqID = byteStringAsUUID(req.getReqId());
        if (!handlers.containsKey(thingReq.getReqCase())) throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
        handlers.get(thingReq.getReqCase()).accept(thingReq, reqID);
    }

    private void delete(ConceptProto.Thing.Req thingReq, UUID reqID) {
        getThing(thingReq).delete();
        transactionSvc.respond(deleteRes(reqID));
    }

    private void getHas(ConceptProto.Thing.Req thingReq, UUID reqID) {
        ConceptProto.Thing.GetHas.Req getHasRequest = thingReq.getThingGetHasReq();
        Thing thing = getThing(thingReq);
        FunctionalIterator<? extends Attribute> attributes;
        if (getHasRequest.hasAttributeTypes())
            attributes = thing.getHas(
                getHasRequest.getAttributeTypes().getAttributeTypesList().stream()
                    .map(t -> notNull(getAttributeType(t))).toArray(AttributeType[]::new)
            );
        else
            attributes = thing.getHas(getAnnotations(getHasRequest.getAnnotationFilter().getAnnotationsList()));
        transactionSvc.stream(attributes, reqID, atts -> getHasResPart(reqID, atts));
    }

    private void setHas(ConceptProto.Thing.Req thingReq, UUID reqID) {
        Thing thing = getThing(thingReq);
        Attribute attribute = getAttribute(thingReq.getThingSetHasReq().getAttribute());
        thing.setHas(attribute);
        transactionSvc.respond(setHasRes(reqID));
    }

    private void unsetHas(ConceptProto.Thing.Req thingReq, UUID reqID) {
        Thing thing = getThing(thingReq);
        Attribute attribute = getAttribute(thingReq.getThingUnsetHasReq().getAttribute());
        thing.unsetHas(attribute);
        transactionSvc.respond(unsetHasRes(reqID));
    }

    private void getRelations(ConceptProto.Thing.Req thingReq, UUID reqID) {
        Thing thing = getThing(thingReq);
        List<ConceptProto.RoleType> protoRoleTypes = thingReq.getThingGetRelationsReq().getRoleTypesList();
        RoleType[] roleTypes = protoRoleTypes.stream().map(type -> notNull(getRoleType(type))).toArray(RoleType[]::new);
        FunctionalIterator<? extends Relation> concepts = thing.getRelations(roleTypes);
        transactionSvc.stream(concepts, reqID, rels -> getRelationsResPart(reqID, rels));
    }

    private void getPlaying(ConceptProto.Thing.Req thingReq, UUID reqID) {
        FunctionalIterator<? extends RoleType> roleTypes = getThing(thingReq).getPlaying();
        transactionSvc.stream(roleTypes, reqID, rols -> getPlayingResPart(reqID, rols));
    }

    private void relationAddPlayer(ConceptProto.Thing.Req thingReq, UUID reqID) {
        Relation relation = getThing(thingReq).asRelation();
        ConceptProto.Relation.AddPlayer.Req addPlayerReq = thingReq.getRelationAddPlayerReq();
        relation.addPlayer(getRoleType(addPlayerReq.getRoleType()), getThing(addPlayerReq.getPlayer()));
        transactionSvc.respond(addPlayerRes(reqID));
    }

    private void relationRemovePlayer(ConceptProto.Thing.Req thingReq, UUID reqID) {
        Relation relation = getThing(thingReq).asRelation();
        ConceptProto.Relation.RemovePlayer.Req removePlayerReq = thingReq.getRelationRemovePlayerReq();
        relation.removePlayer(getRoleType(removePlayerReq.getRoleType()), getThing(removePlayerReq.getPlayer()));
        transactionSvc.respond(removePlayerRes(reqID));
    }

    private void relationGetPlayers(ConceptProto.Thing.Req thingReq, UUID reqID) {
        Relation relation = getThing(thingReq).asRelation();
        RoleType[] roleTypes = thingReq.getRelationGetPlayersReq().getRoleTypesList().stream()
                .map(type -> notNull(getRoleType(type))).toArray(RoleType[]::new);
        FunctionalIterator<? extends Thing> players = roleTypes.length == 0 ?
                relation.getPlayers() :
                relation.getPlayers(roleTypes[0], Arrays.copyOfRange(roleTypes, 1, roleTypes.length));
        transactionSvc.stream(players, reqID, things -> getPlayersResPart(reqID, things));
    }

    private void relationGetPlayersByRoleType(ConceptProto.Thing.Req thingReq, UUID reqID) {
        Relation relation = getThing(thingReq).asRelation();
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

    private void relationGetRelating(ConceptProto.Thing.Req thingReq, UUID reqID) {
        transactionSvc.stream(getThing(thingReq).asRelation().getRelating(), reqID, roleTypes -> getRelatingResPart(reqID, roleTypes));
    }

    private void attributeGetOwners(ConceptProto.Thing.Req thingReq, UUID reqID) {
        Attribute attribute = getThing(thingReq).asAttribute();
        ConceptProto.Attribute.GetOwners.Req getOwnersReq = thingReq.getAttributeGetOwnersReq();
        FunctionalIterator<? extends Thing> things;
        if (getOwnersReq.hasFilter()) things = attribute.getOwners(getThingType(getOwnersReq.getFilter()).asThingType());
        else things = attribute.getOwners();
        transactionSvc.stream(things, reqID, owners -> getOwnersResPart(reqID, owners));
    }

    private void requestNotSet(ConceptProto.Thing.Req thingReq, UUID uuid) {
        throw TypeDBException.of(UNKNOWN_REQUEST_TYPE);
    }

    private static <T extends Concept> T notNull(@Nullable T concept) {
        if (concept == null) throw TypeDBException.of(MISSING_CONCEPT);
        return concept;
    }

    private ThingType getThingType(ConceptProto.ThingType protoThingType) {
        switch (protoThingType.getTypeCase()) {
            case ENTITY_TYPE: return getEntityType(protoThingType.getEntityType()).asThingType();
            case RELATION_TYPE: return getRelationType(protoThingType.getRelationType()).asThingType();
            case ATTRIBUTE_TYPE: return getAttributeType(protoThingType.getAttributeType()).asThingType();
            case TYPE_NOT_SET:
            default:
                throw TypeDBException.of(BAD_VALUE_TYPE, protoThingType.getTypeCase());
        }
    }

    private EntityType getEntityType(ConceptProto.EntityType protoType) {
        return conceptMgr.getEntityType(protoType.getLabel());
    }

    private RelationType getRelationType(ConceptProto.RelationType protoType) {
        return conceptMgr.getRelationType(protoType.getLabel());
    }

    private AttributeType getAttributeType(ConceptProto.AttributeType protoType) {
        return conceptMgr.getAttributeType(protoType.getLabel());
    }

    private RoleType getRoleType(ConceptProto.RoleType protoRoleType) {
        RelationType relationType = conceptMgr.getRelationType(protoRoleType.getScope());
        if (relationType != null) return relationType.getRelates(protoRoleType.getLabel());
        else return null;
    }

    private Thing getThing(ConceptProto.Thing.Req thingReq) {
        return notNull(conceptMgr.getThing(ByteArray.of(thingReq.getIid().toByteArray())));
    }

    private Thing getThing(ConceptProto.Thing protoThing) {
        switch (protoThing.getThingCase()) {
            case ENTITY: return getEntity(protoThing.getEntity()).asThing();
            case RELATION: return getRelation(protoThing.getRelation()).asThing();
            case ATTRIBUTE: return getAttribute(protoThing.getAttribute()).asThing();
            case THING_NOT_SET:
            default:
                throw TypeDBException.of(BAD_VALUE_TYPE, protoThing.getThingCase());
        }
    }

    private Entity getEntity(ConceptProto.Entity protoThing) {
        return conceptMgr.getEntity(ByteArray.of(protoThing.getIid().toByteArray()));
    }

    private Relation getRelation(ConceptProto.Relation protoThing) {
        return conceptMgr.getRelation(ByteArray.of(protoThing.getIid().toByteArray()));
    }

    private Attribute getAttribute(ConceptProto.Attribute protoThing) {
        return conceptMgr.getAttribute(ByteArray.of(protoThing.getIid().toByteArray()));
    }

    private Set<TypeQLToken.Annotation> getAnnotations(List<ConceptProto.Type.Annotation> protoAnnotations) {
        return iterate(protoAnnotations).map(
                annotation -> {
                    switch (annotation.getAnnotationCase()) {
                        case KEY: return TypeQLToken.Annotation.KEY;
                        case UNIQUE: return TypeQLToken.Annotation.UNIQUE;
                        case ANNOTATION_NOT_SET:
                        default: throw TypeDBException.of(ILLEGAL_ARGUMENT);
                    }
                }
        ).toSet();
    }
}
