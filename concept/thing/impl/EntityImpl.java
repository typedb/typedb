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
import hypergraph.concept.thing.Entity;
import hypergraph.concept.type.impl.EntityTypeImpl;
import hypergraph.graph.vertex.impl.ThingVertexImpl;

public class EntityImpl extends ThingImpl implements Entity {

    public EntityImpl(ThingVertexImpl vertex) {
        super(vertex);
    }

    @Override
    public EntityTypeImpl type() {
        return EntityTypeImpl.of(vertex.typeVertex());
    }

    @Override
    public EntityImpl has(Attribute attribute) {
        return null; //TODO
    }
}
