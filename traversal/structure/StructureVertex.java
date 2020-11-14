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

public class StructureVertex {

    public final Set<StructureEdge> outgoing;
    public final Set<StructureEdge> incoming;
    private final Structure structure;
    private final Identifier identifier;
    private final Set<VertexProperty> properties;

    StructureVertex(Identifier identifier, Structure structure) {
        this.structure = structure;
        this.identifier = identifier;
        this.properties = new HashSet<>();
        this.outgoing = new HashSet<>();
        this.incoming = new HashSet<>();
    }

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

    public Set<VertexProperty> properties() {
        return properties;
    }

    public void property(VertexProperty property) {
        properties.add(property);
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

        StructureVertex that = (StructureVertex) o;
        return (this.identifier.equals(that.identifier) && this.properties.equals(that.properties));
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, properties);
    }

    public static class Thing extends StructureVertex {

        public Thing(Identifier identifier, Structure structure) {
            super(identifier, structure);
        }

        public boolean isThing() {
            return true;
        }

        public StructureVertex.Thing asThing() {
            return this;
        }
    }

    public static class Type extends StructureVertex {

        public Type(Identifier identifier, Structure structure) {
            super(identifier, structure);
        }

        public boolean isType() {
            return true;
        }

        public StructureVertex.Type asType() {
            return this;
        }
    }

}
