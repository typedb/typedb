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

import hypergraph.concept.thing.Attribute;
import hypergraph.concept.thing.Relation;
import hypergraph.concept.type.impl.RelationTypeImpl;
import hypergraph.graph.vertex.ThingVertex;

import java.util.HashSet;
import java.util.Set;

public class RelationImpl extends ThingImpl implements Relation {

    private final Set<RoleImpl> roles;

    private RelationImpl(ThingVertex vertex) {
        super(vertex);
        this.roles = new HashSet<>();
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
    public void validate() {
        // TODO: validate relation
    }
}
