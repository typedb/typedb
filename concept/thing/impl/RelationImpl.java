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

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.impl.RelationTypeImpl;
import grakn.core.concept.type.impl.RoleTypeImpl;
import grakn.core.graph.iid.PrefixIID;
import grakn.core.graph.util.Schema;
import grakn.core.graph.vertex.ThingVertex;
import grakn.core.graph.vertex.TypeVertex;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static grakn.core.common.exception.ErrorMessage.ThingWrite.RELATION_NO_PLAYER;
import static grakn.core.common.iterator.Iterators.filter;
import static grakn.core.common.iterator.Iterators.stream;
import static java.util.stream.Collectors.toList;

public class RelationImpl extends ThingImpl implements Relation {

    private RelationImpl(ThingVertex vertex) {
        super(vertex);
    }

    public static RelationImpl of(ThingVertex vertex) {
        return new RelationImpl(vertex);
    }

    @Override
    public RelationTypeImpl getType() {
        return RelationTypeImpl.of(vertex.type());
    }

    @Override
    public RelationImpl setHas(Attribute attribute) {
        return (RelationImpl) super.setHas(attribute).asRelation();
    }

    @Override
    public void addPlayer(RoleType roleType, Thing player) {
        if (this.getType().getRelates().noneMatch(t -> t.equals(roleType))) {
            throw new GraknException(
                    ErrorMessage.ThingWrite.RELATION_UNRELATED_ROLE.message(this.getType().getLabel(), roleType.getLabel())
            );
        }

        RoleImpl role = ((RoleTypeImpl) roleType).create();
        vertex.outs().put(Schema.Edge.Thing.RELATES, role.vertex);
        ((ThingImpl) player).vertex.outs().put(Schema.Edge.Thing.PLAYS, role.vertex);
        role.optimise();
    }

    @Override
    public void removePlayer(RoleType roleType, Thing player) {
        Iterator<ThingVertex> role = filter(
                vertex.outs().edge(Schema.Edge.Thing.RELATES,
                                   PrefixIID.of(Schema.Vertex.Thing.ROLE),
                                   ((RoleTypeImpl) roleType).vertex.iid()).to(),
                v -> v.ins().edge(Schema.Edge.Thing.PLAYS, ((ThingImpl) player).vertex) != null
        );
        if (role.hasNext()) {
            RoleImpl.of(role.next()).delete();
            if (!vertex.outs().edge(Schema.Edge.Thing.RELATES).to().hasNext()) this.delete();
        }
    }

    @Override
    public Stream<ThingImpl> getPlayers() {
        return stream(vertex.outs().edge(Schema.Edge.Thing.ROLEPLAYER).to()).map(ThingImpl::of);
    }

    @Override
    public Stream<ThingImpl> getPlayers(RoleType roleType) {
        return getPlayers(roleType.getSubtypes().map(rt -> ((RoleTypeImpl) rt).vertex));
    }

    @Override
    public Stream<ThingImpl> getPlayers(List<RoleType> roleTypes) {
        return getPlayers(roleTypes.stream().flatMap(RoleType::getSubtypes).distinct().map(rt -> ((RoleTypeImpl) rt).vertex));
    }

    private Stream<ThingImpl> getPlayers(Stream<TypeVertex> roleTypeVertices) {
        return roleTypeVertices.flatMap(v -> stream(
                vertex.outs().edge(Schema.Edge.Thing.ROLEPLAYER, v.iid()).to())
        ).map(ThingImpl::of);
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
        if (!vertex.outs().edge(Schema.Edge.Thing.RELATES).to().hasNext()) {
            throw new GraknException(RELATION_NO_PLAYER.message(getType().getLabel()));
        }
    }
}
