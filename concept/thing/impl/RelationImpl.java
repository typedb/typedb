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

package hypergraph.concept.thing.impl;

import hypergraph.common.exception.Error;
import hypergraph.common.exception.HypergraphException;
import hypergraph.concept.thing.Attribute;
import hypergraph.concept.thing.Relation;
import hypergraph.concept.thing.Thing;
import hypergraph.concept.type.RoleType;
import hypergraph.concept.type.impl.RelationTypeImpl;
import hypergraph.concept.type.impl.RoleTypeImpl;
import hypergraph.graph.iid.PrefixIID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.vertex.ThingVertex;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static hypergraph.common.exception.Error.ThingWrite.RELATION_NO_PLAYER;
import static hypergraph.common.iterator.Iterators.filter;
import static hypergraph.common.iterator.Iterators.stream;

public class RelationImpl extends ThingImpl implements Relation {

    private RelationImpl(ThingVertex vertex) {
        super(vertex);
    }

    public static RelationImpl of(ThingVertex vertex) {
        return new RelationImpl(vertex);
    }

    @Override
    public RelationTypeImpl type() {
        return RelationTypeImpl.of(vertex.type());
    }

    @Override
    public RelationImpl has(Attribute attribute) {
        return (RelationImpl) super.has(attribute).asRelation();
    }

    @Override
    public RelationImpl relate(RoleType roleType, Thing player) {
        if (this.type().roles().noneMatch(t -> t.equals(roleType))) {
            throw new HypergraphException(
                    Error.ThingWrite.RELATION_UNRELATED_ROLE.format(this.type().label(), roleType.label())
            );
        }

        RoleImpl role = ((RoleTypeImpl) roleType).create();
        vertex.outs().put(Schema.Edge.Thing.RELATES, role.vertex);
        ((ThingImpl) player).vertex.outs().put(Schema.Edge.Thing.PLAYS, role.vertex);
        role.optimise();
        return this;
    }

    @Override
    public void unrelate(RoleType roleType, Thing player) {
        Iterator<ThingVertex> role = filter(
                vertex.outs().edge(Schema.Edge.Thing.RELATES,
                                   PrefixIID.of(Schema.Vertex.Thing.ROLE.prefix()),
                                   ((RoleTypeImpl) roleType).vertex.iid()).to(),
                v -> v.ins().edge(Schema.Edge.Thing.PLAYS, ((ThingImpl) player).vertex) != null
        );
        if (role.hasNext()) {
            RoleImpl.of(role.next()).delete();
            if (!vertex.outs().edge(Schema.Edge.Thing.RELATES).to().hasNext()) this.delete();
        }
    }

    @Override
    public Stream<? extends ThingImpl> players() {
        return stream(vertex.outs().edge(Schema.Edge.Thing.ROLEPLAYER).to()).map(ThingImpl::of);
    }

    @Override
    public Stream<? extends ThingImpl> players(RoleType roleType) {
        return stream(vertex.outs().edge(Schema.Edge.Thing.ROLEPLAYER, ((RoleTypeImpl) roleType).vertex.iid()).to())
                .map(ThingImpl::of);
    }

    @Override
    public Stream<? extends ThingImpl> players(List<RoleType> roleTypes) {
        return roleTypes.stream().flatMap(this::players);
    }

    @Override
    public void validate() {
        super.validate();
        if (!vertex.outs().edge(Schema.Edge.Thing.RELATES).to().hasNext()) {
            throw new HypergraphException(RELATION_NO_PLAYER.format(type().label()));
        }
    }
}
