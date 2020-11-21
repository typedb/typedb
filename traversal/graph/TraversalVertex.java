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

package grakn.core.traversal.graph;

import grakn.core.common.parameters.Label;
import grakn.core.graph.util.Encoding;
import grakn.core.traversal.Identifier;
import graql.lang.common.GraqlToken;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public abstract class TraversalVertex<EDGE extends TraversalEdge<?>, PROPERTIES extends TraversalVertex.Properties> {

    private final Identifier identifier;
    private final Set<EDGE> outgoing;
    private final Set<EDGE> incoming;
    private final int hash;
    private PROPERTIES properties;

    public TraversalVertex(Identifier identifier) {
        this.identifier = identifier;
        this.outgoing = new HashSet<>();
        this.incoming = new HashSet<>();
        this.properties = newProperties();
        this.hash = Objects.hash(identifier);
    }

    protected abstract PROPERTIES newProperties();

    public boolean isThing() { return false; }

    public boolean isType() { return false; }

    public Identifier identifier() {
        return identifier;
    }

    public Set<EDGE> outs() {
        return outgoing;
    }

    public Set<EDGE> ins() {
        return incoming;
    }

    public void out(EDGE edge) {
        assert edge.from().equals(this);
        outgoing.add(edge);
    }

    public void in(EDGE edge) {
        assert edge.to().equals(this);
        incoming.add(edge);
    }

    public PROPERTIES properties() {
        return properties;
    }

    public void properties(PROPERTIES properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return identifier.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TraversalVertex<?, ?> that = (TraversalVertex<?, ?>) o;
        return this.identifier.equals(that.identifier);
        // We do not consider the properties stored in the vertex as doing that
        // would result in an inconsistent identity (hash/equality)
        // (and screw up with HashMaps or HashSets)
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static abstract class Properties {

        @Override
        public abstract String toString();

        public static class Thing extends Properties {

            private boolean hasIID;
            private final Set<Label> types;
            private final List<GraqlToken.Comparator> comparators;

            public Thing() {
                types = new HashSet<>();
                comparators = new ArrayList<>();
            }

            public boolean hasIID() {
                return hasIID;
            }

            public void hasIID(boolean hasIID) {
                this.hasIID = hasIID;
            }

            public Set<Label> types() {
                return types;
            }

            public void types(Set<Label> types) {
                this.types.addAll(types);
            }

            public List<GraqlToken.Comparator> values() {
                return comparators;
            }

            public void value(GraqlToken.Comparator comparator) {
                comparators.add(comparator);
            }

            @Override
            public String toString() {
                return String.format("Thing Vertex { hasIID: %s, types: %s, comparators: %s }",
                                     hasIID, types, comparators);
            }
        }

        public static class Type extends Properties {

            private Label label;
            private boolean isAbstract;
            private Encoding.ValueType valueType;
            private String regex;

            public Type() {}

            public Optional<Label> label() {
                return Optional.ofNullable(label);
            }

            public void label(Label label) {
                this.label = label;
            }

            public boolean isAbstract() {
                return isAbstract;
            }

            public void isAbstract(boolean isAbstract) {
                this.isAbstract = isAbstract;
            }

            public Optional<Encoding.ValueType> valueType() {
                return Optional.ofNullable(valueType);
            }

            public void valueType(Encoding.ValueType valueType) {
                this.valueType = valueType;
            }

            public Optional<String> regex() {
                return Optional.ofNullable(regex);
            }

            public void regex(String regex) {
                this.regex = regex;
            }

            @Override
            public String toString() {
                return String.format("Type Vertex { label: %s, abstract: %s, value: %s, regex: %s }",
                                     label, isAbstract, valueType, regex);
            }
        }
    }
}
