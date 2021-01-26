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
 *
 */

package grakn.core.concept.thing.impl;

import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.impl.RelationTypeImpl;
import grakn.core.concept.type.impl.RoleTypeImpl;
import grakn.core.graph.iid.PrefixIID;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.ThingWrite.RELATION_PLAYER_MISSING;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.RELATION_ROLE_UNRELATED;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.THING_ROLE_UNPLAYED;
import static grakn.core.common.exception.ErrorMessage.ThingWrite.DELETE_ROLEPLAYER_NOT_PRESENT;
import static grakn.core.graph.common.Encoding.Edge.Thing.PLAYING;
import static grakn.core.graph.common.Encoding.Edge.Thing.RELATING;
import static grakn.core.graph.common.Encoding.Edge.Thing.ROLEPLAYER;
import static grakn.core.graph.common.Encoding.Vertex.Thing.ROLE;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class RelationImpl extends ThingImpl implements Relation {

    private RelationImpl(ThingVertex vertex) {
        super(vertex);
    }

    public static RelationImpl of(ThingVertex vertex) {
        return new RelationImpl(vertex);
    }

    @Override
    public RelationTypeImpl getType() {
        return RelationTypeImpl.of(vertex.graphs(), vertex.type());
    }

    @Override
    public void addPlayer(RoleType roleType, Thing player) {
        addPlayer(roleType, player, false);
    }

    @Override
    public void addPlayer(RoleType roleType, Thing player, boolean isInferred) {
        assert isInferred() == isInferred;
        if (this.getType().getRelates().noneMatch(t -> t.equals(roleType))) {
            throw exception(GraknException.of(RELATION_ROLE_UNRELATED, this.getType().getLabel(), roleType.getLabel()));
        } else if (player.getType().getPlays().noneMatch(t -> t.equals(roleType))) {
            throw exception(GraknException.of(THING_ROLE_UNPLAYED, player.getType().getLabel(), roleType.getLabel().toString()));
        }

        RoleImpl role = ((RoleTypeImpl) roleType).create(isInferred);
        vertex.outs().put(RELATING, role.vertex, isInferred);
        ((ThingImpl) player).vertex.outs().put(PLAYING, role.vertex, isInferred);
        role.optimise();
    }

    @Override
    public void removePlayer(RoleType roleType, Thing player) {
        ResourceIterator<ThingVertex> role = vertex.outs().edge(
                RELATING, PrefixIID.of(ROLE), ((RoleTypeImpl) roleType).vertex.iid()
        ).to().filter(v -> v.ins().edge(PLAYING, ((ThingImpl) player).vertex) != null);
        if (role.hasNext()) {
            RoleImpl.of(role.next()).delete();
            deleteIfNoPlayer();
        } else {
            throw exception(GraknException.of(DELETE_ROLEPLAYER_NOT_PRESENT, player.getType().getLabel(), roleType.getLabel().toString()));
        }
    }

    @Override
    public void delete() {
        vertex.outs().edge(RELATING).to().map(RoleImpl::of).forEachRemaining(RoleImpl::delete);
        super.delete();
    }

    void deleteIfNoPlayer() {
        if (!vertex.outs().edge(RELATING).to().hasNext()) this.delete();
    }

    @Override
    public Stream<ThingImpl> getPlayers(String roleType, String... roleTypes) {
        return getPlayers(concat(Stream.of(roleType), stream(roleTypes))
                                  .map(label -> getType().getRelates(label))
                                  .toArray(RoleType[]::new));
    }

    @Override
    public Stream<ThingImpl> getPlayers(RoleType... roleTypes) {
        if (roleTypes.length == 0) {
            return vertex.outs().edge(ROLEPLAYER).to().stream().map(ThingImpl::of);
        }
        return getPlayers(stream(roleTypes).flatMap(RoleType::getSubtypes).distinct().map(rt -> ((RoleTypeImpl) rt).vertex));
    }

    private Stream<ThingImpl> getPlayers(Stream<TypeVertex> roleTypeVertices) {
        return roleTypeVertices.flatMap(v -> vertex.outs().edge(ROLEPLAYER, v.iid()).to().stream()).map(ThingImpl::of);
    }

    @Override
    public Map<RoleTypeImpl, ? extends List<ThingImpl>> getPlayersByRoleType() {
        Map<RoleTypeImpl, List<ThingImpl>> playersByRole = new HashMap<>();
        getType().getRelates().forEach(rt -> {
            List<ThingImpl> players = getPlayers(rt).collect(toList());
            if (!players.isEmpty()) playersByRole.put(rt, players);
        });
        return playersByRole;
    }

    @Override
    public void validate() {
        super.validate();
        if (!vertex.outs().edge(RELATING).to().hasNext()) {
            throw exception(GraknException.of(RELATION_PLAYER_MISSING, getType().getLabel()));
        }
    }

    @Override
    public boolean isRelation() {
        return true;
    }

    @Override
    public RelationImpl asRelation() {
        return this;
    }
}
