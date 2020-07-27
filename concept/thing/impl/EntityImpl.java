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

import grakn.core.concept.thing.Attribute;
import grakn.core.concept.thing.Entity;
import grakn.core.concept.type.impl.EntityTypeImpl;
import grakn.core.graph.util.Schema;
import grakn.core.graph.vertex.ThingVertex;

public class EntityImpl extends ThingImpl implements Entity {

    private EntityImpl(ThingVertex vertex) {
        super(vertex);
        assert vertex.schema().equals(Schema.Vertex.Thing.ENTITY);
    }

    public static EntityImpl of(ThingVertex vertex) {
        return new EntityImpl(vertex);
    }

    @Override
    public EntityTypeImpl type() {
        return EntityTypeImpl.of(vertex.type());
    }

    @Override
    public EntityImpl has(Attribute attribute) {
        return (EntityImpl) super.has(attribute).asEntity();
    }

    @Override
    public void validate() {
        super.validate();
    }
}
