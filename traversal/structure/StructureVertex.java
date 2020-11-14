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
import grakn.core.traversal.property.VertexProperty;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class StructureVertex {

    public final Set<StructureEdge> outgoing;
    public final Set<StructureEdge> incoming;
    private final Structure structure;
    final Identifier identifier;

    StructureVertex(Identifier identifier, Structure structure) {
        this.structure = structure;
        this.identifier = identifier;
        this.outgoing = new HashSet<>();
        this.incoming = new HashSet<>();
    }

    public abstract Set<? extends VertexProperty> properties();

    void out(StructureEdge edge) {
        outgoing.add(edge);
    }

    void in(StructureEdge edge) {
        incoming.add(edge);
    }

    public Set<StructureEdge> outs() {
        return outgoing;
    }

    public Set<StructureEdge> ins() {
        return incoming;
    }

    public Identifier identifier() {
        return identifier;
    }

    public boolean isThing() {
        return false;
    }

    public boolean isType() {
        return false;
    }

    public StructureVertex.Thing asThing() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(StructureVertex.Thing.class)));
    }

    public StructureVertex.Type asType() {
        throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(StructureVertex.Type.class)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StructureVertex.Thing that = (StructureVertex.Thing) o;
        return (this.identifier.equals(that.identifier) && Objects.equals(this.properties(), that.properties()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, this.properties());
    }

    public static class Thing extends StructureVertex {

        private final Set<VertexProperty.Thing> properties;

        public Thing(Identifier identifier, Structure structure) {
            super(identifier, structure);
            this.properties = new HashSet<>();
        }

        @Override
        public boolean isThing() {
            return true;
        }

        @Override
        public StructureVertex.Thing asThing() {
            return this;
        }

        @Override
        public Set<VertexProperty.Thing> properties() {
            return properties;
        }

        public void property(VertexProperty.Thing property) {
            properties.add(property);
        }
    }

    public static class Type extends StructureVertex {

        private final Set<VertexProperty.Type> properties;

        public Type(Identifier identifier, Structure structure) {
            super(identifier, structure);
            this.properties = new HashSet<>();
        }

        @Override
        public boolean isType() {
            return true;
        }

        @Override
        public StructureVertex.Type asType() {
            return this;
        }

        @Override
        public Set<VertexProperty.Type> properties() {
            return properties;
        }

        public void property(VertexProperty.Type property) {
            properties.add(property);
        }
    }
}
