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
 *
 */

package com.vaticle.typedb.core.concept.thing.impl;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.thing.Relation;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.RoleType;
import com.vaticle.typedb.core.concept.type.impl.RelationTypeImpl;
import com.vaticle.typedb.core.concept.type.impl.RoleTypeImpl;
import com.vaticle.typedb.core.graph.iid.PrefixIID;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.DELETE_ROLEPLAYER_NOT_PRESENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.RELATION_PLAYER_MISSING;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.RELATION_ROLE_UNRELATED;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.ThingWrite.THING_ROLE_UNPLAYED;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static com.vaticle.typedb.core.common.iterator.Iterators.single;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.PLAYING;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.RELATING;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.ROLEPLAYER;
import static com.vaticle.typedb.core.graph.common.Encoding.Vertex.Thing.ROLE;

public class RelationImpl extends ThingImpl implements Relation {

    private RelationImpl(ThingVertex vertex) {
        super(vertex);
    }

    public static RelationImpl of(ThingVertex vertex) {
        return new RelationImpl(vertex);
    }

    @Override
    public RelationTypeImpl getType() {
        return RelationTypeImpl.of(vertex().graphs(), vertex().type());
    }

    @Override
    public void addPlayer(RoleType roleType, Thing player) {
        addPlayer(roleType, player, false);
    }

    @Override
    public void addPlayer(RoleType roleType, Thing player, boolean isInferred) {
        assert isInferred() == isInferred;
        validateIsNotDeleted();
        if (this.getType().getRelates().noneMatch(t -> t.equals(roleType))) {
            throw exception(TypeDBException.of(RELATION_ROLE_UNRELATED, this.getType().getLabel(), roleType.getLabel()));
        } else if (player.getType().getPlays().noneMatch(t -> t.equals(roleType))) {
            throw exception(TypeDBException.of(THING_ROLE_UNPLAYED, player.getType().getLabel(), roleType.getLabel().toString()));
        }

        RoleImpl role = ((RoleTypeImpl) roleType).create(isInferred);
        vertexWritable().outs().put(RELATING, role.vertex, isInferred);
        ((ThingImpl) player).vertexWritable().outs().put(PLAYING, role.vertex, isInferred);
        role.optimise();
    }

    @Override
    public void removePlayer(RoleType roleType, Thing player) {
        validateIsNotDeleted();
        FunctionalIterator<ThingVertex> role = vertexWritable().outs().edge(
                RELATING, PrefixIID.of(ROLE), ((RoleTypeImpl) roleType).vertex().iid()
        ).to().filter(v -> v.ins().edge(PLAYING, ((ThingImpl) player).vertexWritable()) != null);
        if (role.hasNext()) {
            RoleImpl.of(role.next().writable()).delete();
            deleteIfNoPlayer();
        } else {
            throw exception(TypeDBException.of(DELETE_ROLEPLAYER_NOT_PRESENT, player.getType().getLabel(), roleType.getLabel().toString()));
        }
    }

    @Override
    public void delete() {
        vertexWritable().outs().edge(RELATING).to().map(v -> RoleImpl.of(v.writable())).forEachRemaining(RoleImpl::delete);
        super.delete();
    }

    void deleteIfNoPlayer() {
        if (!vertexWritable().outs().edge(RELATING).to().hasNext()) this.delete();
    }

    @Override
    public FunctionalIterator<ThingImpl> getPlayers(String roleType, String... roleTypes) {
        return getPlayers(link(single(roleType), iterate(roleTypes))
                                  .map(label -> getType().getRelates(label))
                                  .stream().toArray(RoleType[]::new));
    }

    @Override
    public FunctionalIterator<ThingImpl> getPlayers(RoleType... roleTypes) {
        if (roleTypes.length == 0) {
            return vertex().outs().edge(ROLEPLAYER).to().map(ThingImpl::of);
        }
        return getPlayers(iterate(roleTypes).flatMap(RoleType::getSubtypes).distinct().map(rt -> ((RoleTypeImpl) rt).vertex()));
    }

    private FunctionalIterator<ThingImpl> getPlayers(FunctionalIterator<TypeVertex> roleTypeVertices) {
        return roleTypeVertices.flatMap(v -> vertex().outs().edge(ROLEPLAYER, v.iid()).to()).map(ThingImpl::of);
    }

    @Override
    public Map<RoleTypeImpl, ? extends List<ThingImpl>> getPlayersByRoleType() {
        Map<RoleTypeImpl, List<ThingImpl>> playersByRole = new HashMap<>();
        getType().getRelates().forEachRemaining(rt -> {
            List<ThingImpl> players = getPlayers(rt).toList();
            if (!players.isEmpty()) playersByRole.put(rt, players);
        });
        return playersByRole;
    }

    @Override
    public FunctionalIterator<? extends RoleType> getRelating() {
        return vertex().outs().edge(RELATING).to().map(ThingVertex::type)
                .map(v -> RoleTypeImpl.of(vertex().graphs(), v))
                .distinct();
    }

    @Override
    public void validate() {
        super.validate();
        if (!vertex().outs().edge(RELATING).to().hasNext()) {
            throw exception(TypeDBException.of(RELATION_PLAYER_MISSING, getType().getLabel()));
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
