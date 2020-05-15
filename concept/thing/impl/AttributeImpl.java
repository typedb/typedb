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
import hypergraph.concept.type.impl.AttributeTypeImpl;
import hypergraph.graph.vertex.ThingVertex;

public class AttributeImpl extends ThingImpl implements Attribute {
    public AttributeImpl(ThingVertex vertex) {
        super(vertex);
    }

    @Override
    public AttributeTypeImpl type() {
        return AttributeTypeImpl.of(vertex.typeVertex());
    }

    @Override
    public AttributeImpl has(Attribute attribute) {
        return null; //TODO
    }
}
