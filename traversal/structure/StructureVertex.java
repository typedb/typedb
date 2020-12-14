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
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.graph.TraversalVertex;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class StructureVertex<PROPERTY extends TraversalVertex.Properties>
        extends TraversalVertex<StructureEdge<?, ?>, PROPERTY> {

    final Structure structure;

    StructureVertex(Structure structure, Identifier identifier) {
        super(identifier);
        this.structure = structure;
    }

    public StructureVertex.Thing asThing() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(StructureVertex.Thing.class));
    }

    public StructureVertex.Type asType() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(StructureVertex.Type.class));
    }

    public static class Thing extends StructureVertex<TraversalVertex.Properties.Thing> {

        Thing(Structure structure, Identifier identifier) {
            super(structure, identifier);
        }

        @Override
        protected Properties.Thing newProperties() {
            return new Properties.Thing();
        }

        @Override
        public boolean isThing() { return true; }

        @Override
        public StructureVertex.Thing asThing() { return this; }
    }

    public static class Type extends StructureVertex<Properties.Type> {

        Type(Structure structure, Identifier identifier) {
            super(structure, identifier);
        }

        @Override
        protected Properties.Type newProperties() {
            return new Properties.Type();
        }

        @Override
        public boolean isType() { return true; }

        @Override
        public StructureVertex.Type asType() { return this; }
    }
}
