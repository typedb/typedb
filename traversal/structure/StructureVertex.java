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

package grakn.core.traversal.structure;

import grakn.core.common.exception.GraknException;
import grakn.core.traversal.Identifier;
import grakn.core.traversal.graph.TraversalVertex;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public abstract class StructureVertex<PROPERTY extends TraversalVertex.Property>
        extends TraversalVertex<StructureEdge, PROPERTY> {

    StructureVertex(Identifier identifier) {
        super(identifier);
    }

    public StructureVertex.Thing asThing() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(StructureVertex.Thing.class)));
    }

    public StructureVertex.Type asType() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(StructureVertex.Type.class)));
    }

    public static class Thing extends StructureVertex<TraversalVertex.Property.Thing> {

        private TraversalVertex.Property.Thing.Isa isa;

        Thing(Identifier identifier) {
            super(identifier);
        }

        @Override
        public boolean isThing() { return true; }

        @Override
        public StructureVertex.Thing asThing() { return this; }

        @Override
        public void property(TraversalVertex.Property.Thing property) {
            if (property.isIsa()) {
                if (isa != null && !isa.equals(property)) throw GraknException.of(ILLEGAL_STATE);
                isa = property.asIsa();
            }
            properties.add(property);
        }
    }

    public static class Type extends StructureVertex<TraversalVertex.Property.Type> {

        Type(Identifier identifier) {
            super(identifier);
        }

        @Override
        public boolean isType() { return true; }

        @Override
        public StructureVertex.Type asType() { return this; }

        public void property(TraversalVertex.Property.Type property) {
            properties.add(property);
        }
    }
}
