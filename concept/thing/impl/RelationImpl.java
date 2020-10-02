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
 *
 */

package grakn.core.concept.thing.impl;

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
import static grakn.core.graph.util.Encoding.Edge.Thing.PLAYS;
import static grakn.core.graph.util.Encoding.Edge.Thing.RELATES;
import static grakn.core.graph.util.Encoding.Edge.Thing.ROLEPLAYER;
import static grakn.core.graph.util.Encoding.Vertex.Thing.ROLE;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class RelationImpl extends ThingImpl implements Relation {

    private RelationImpl(final ThingVertex vertex) {
        super(vertex);
    }

    public static RelationImpl of(final ThingVertex vertex) {
        return new RelationImpl(vertex);
    }

    @Override
    public RelationTypeImpl getType() {
        return RelationTypeImpl.of(vertex.graphs(), vertex.type());
    }

    @Override
    public void addPlayer(final RoleType roleType, final Thing player) {
        if (this.getType().getRelates().noneMatch(t -> t.equals(roleType))) {
            throw exception(RELATION_ROLE_UNRELATED.message(this.getType().getLabel(), roleType.getLabel()));
        } else if (player.getType().getPlays().noneMatch(t -> t.equals(roleType))) {
            throw exception(THING_ROLE_UNPLAYED.message(this.getType().getLabel(), roleType.getLabel()));
        }

        final RoleImpl role = ((RoleTypeImpl) roleType).create();
        vertex.outs().put(RELATES, role.vertex);
        ((ThingImpl) player).vertex.outs().put(PLAYS, role.vertex);
        role.optimise();
    }

    @Override
    public void removePlayer(final RoleType roleType, final Thing player) {
        final ResourceIterator<ThingVertex> role = vertex.outs().edge(
                RELATES, PrefixIID.of(ROLE), ((RoleTypeImpl) roleType).vertex.iid()
        ).to().filter(v -> v.ins().edge(PLAYS, ((ThingImpl) player).vertex) != null);

        if (role.hasNext()) {
            RoleImpl.of(role.next()).delete();
            if (!vertex.outs().edge(RELATES).to().hasNext()) this.delete();
        }
    }

    @Override
    public Stream<ThingImpl> getPlayers(final String roleType, final String... roleTypes) {
        return getPlayers(concat(Stream.of(roleType), stream(roleTypes))
                                  .map(label -> getType().getRelates(label))
                                  .toArray(RoleType[]::new));
    }

    @Override
    public Stream<ThingImpl> getPlayers(final RoleType... roleTypes) {
        if (roleTypes.length == 0) {
            return vertex.outs().edge(ROLEPLAYER).to().stream().map(ThingImpl::of);
        }
        return getPlayers(stream(roleTypes).flatMap(RoleType::getSubtypes).distinct().map(rt -> ((RoleTypeImpl) rt).vertex));
    }

    private Stream<ThingImpl> getPlayers(final Stream<TypeVertex> roleTypeVertices) {
        return roleTypeVertices.flatMap(v -> vertex.outs().edge(ROLEPLAYER, v.iid()).to().stream()).map(ThingImpl::of);
    }

    @Override
    public Map<RoleTypeImpl, ? extends List<ThingImpl>> getPlayersByRoleType() {
        final Map<RoleTypeImpl, List<ThingImpl>> playersByRole = new HashMap<>();
        getType().getRelates().forEach(rt -> {
            final List<ThingImpl> players = getPlayers(rt).collect(toList());
            if (!players.isEmpty()) playersByRole.put(rt, players);
        });
        return playersByRole;
    }

    @Override
    public void validate() {
        super.validate();
        if (!vertex.outs().edge(RELATES).to().hasNext()) {
            throw exception(RELATION_PLAYER_MISSING.message(getType().getLabel()));
        }
    }

    @Override
    public RelationImpl asRelation() {
        return this;
    }
}
