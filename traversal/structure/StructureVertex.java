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

import grakn.core.graph.util.Encoding;
import grakn.core.traversal.Identifier;
import graql.lang.common.GraqlToken;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class StructureVertex {

    public final Set<StructureEdge> outgoing;
    public final Set<StructureEdge> incoming;
    private final Structure structure;
    private final Identifier identifier;
    private final Set<Property> properties;

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

    public Set<Property> properties() {
        return properties;
    }

    public void property(Property property) {
        properties.add(property);
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

    public abstract static class Property {

        public static class Abstract extends Property {

            private final int hash;

            public Abstract() {
                this.hash = Objects.hash(getClass());
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                return o != null && getClass() == o.getClass();
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        public static class IID extends Property {

            private final Identifier param;
            private final int hash;

            public IID(Identifier param) {
                this.param = param;
                this.hash = Objects.hash(this.param);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                IID that = (IID) o;
                return this.param.equals(that.param);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        public static class Label extends Property {

            private final String label, scope;
            private final int hash;

            public Label(String label, @Nullable String scope) {
                this.label = label;
                this.scope = scope;
                this.hash = Objects.hash(this.label, this.scope);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Label that = (Label) o;
                return (this.label.equals(that.label) && Objects.equals(this.scope, that.scope));
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        public static class Regex extends Property {

            private final String regex;
            private final int hash;

            public Regex(String regex) {
                this.regex = regex;
                this.hash = Objects.hash(this.regex);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Regex that = (Regex) o;
                return this.regex.equals(that.regex);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        public static class Type extends Property {

            private final String[] labels;
            private final int hash;

            public Type(String[] labels) {
                this.labels = labels;
                this.hash = Arrays.hashCode(this.labels);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Type that = (Type) o;
                return Arrays.equals(this.labels, that.labels);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        public static class ValueType extends Property {

            private final Encoding.ValueType valueType;
            private final int hash;

            public ValueType(Encoding.ValueType valueType) {
                this.valueType = valueType;
                this.hash = Objects.hash(this.valueType);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                ValueType that = (ValueType) o;
                return this.valueType.equals(that.valueType);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        public static class Value extends Property {

            private final GraqlToken.Comparator comparator;
            private final Identifier param;
            private final int hash;

            public Value(GraqlToken.Comparator comparator, Identifier param) {
                this.comparator = comparator;
                this.param = param;
                this.hash = Objects.hash(this.comparator, this.param);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Value that = (Value) o;
                return (this.comparator.equals(that.comparator) && this.param.equals(that.param));
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }
    }
}
