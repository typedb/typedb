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

import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Label;
import grakn.core.graph.util.Encoding;
import grakn.core.traversal.Identifier;
import graql.lang.common.GraqlToken;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class TraversalVertex<EDGE extends TraversalEdge<?>, PROPERTY extends TraversalVertex.Property> {

    private final Identifier identifier;
    private final Set<EDGE> outgoing;
    private final Set<EDGE> incoming;
    private final int hash;

    protected final Set<PROPERTY> properties;

    public TraversalVertex(Identifier identifier) {
        this.identifier = identifier;
        this.outgoing = new HashSet<>();
        this.incoming = new HashSet<>();
        this.properties = new HashSet<>();
        this.hash = Objects.hash(identifier);
    }

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

    public Set<PROPERTY> properties() {
        return properties;
    }

    public void property(PROPERTY property) {
        properties.add(property);
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
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static abstract class Property {

        public boolean isIndexed() { return false; }

        @Override
        public abstract String toString();

        public static abstract class Thing extends Property {

            public boolean isIID() { return false; }

            public boolean isIsa() { return false; }

            public boolean isValue() { return false; }

            public Thing.IID asIID() {
                throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Thing.IID.class)));
            }

            public Thing.Isa asIsa() {
                throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Thing.Isa.class)));
            }

            public Thing.Value asValue() {
                throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Thing.Value.class)));
            }

            public static class IID extends Thing {

                private final Identifier param;
                private final int hash;

                public IID(Identifier param) {
                    this.param = param;
                    this.hash = Objects.hash(this.param);
                }

                @Override
                public boolean isIID() { return true; }

                @Override
                public IID asIID() { return this; }

                @Override
                public String toString() {
                    return String.format("Thing Property: IID { param: %s }", param);
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

            public static class Isa extends Thing {

                private final Label[] labels;
                private final int hash;

                public Isa(Label[] labels) {
                    this.labels = labels;
                    this.hash = Arrays.hashCode(this.labels);
                }

                @Override
                public boolean isIsa() { return true; }

                @Override
                public Isa asIsa() { return this; }

                @Override
                public String toString() {
                    return String.format("Thing Property: Isa { labels: %s }", Arrays.toString(labels));
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;

                    Isa that = (Isa) o;
                    return Arrays.equals(this.labels, that.labels);
                }

                @Override
                public int hashCode() {
                    return hash;
                }
            }

            public static class Value extends Thing {

                private final GraqlToken.Comparator comparator;
                private final Identifier param;
                private final int hash;

                public Value(GraqlToken.Comparator comparator, Identifier param) {
                    this.comparator = comparator;
                    this.param = param;
                    this.hash = Objects.hash(this.comparator, this.param);
                }

                @Override
                public boolean isValue() { return true; }

                @Override
                public Value asValue() { return this; }

                @Override
                public String toString() {
                    return String.format("Thing Property: Value { comparator: %s, param: %s }", comparator, param);
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

        public static abstract class Type extends Property {

            public boolean isLabel() { return false; }

            public boolean isAbstract() { return false; }

            public boolean isValueType() { return false; }

            public boolean isRegex() { return false; }

            public Type.Label asLabel() {
                throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Type.Label.class)));
            }

            public Type.Abstract asAbstract() {
                throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Type.Abstract.class)));
            }

            public Type.ValueType asValueType() {
                throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Type.ValueType.class)));
            }

            public Type.Regex asRegex() {
                throw GraknException.of(ILLEGAL_CAST.message(className(this.getClass()), className(Type.Regex.class)));
            }

            public static class Label extends Type {

                private final String label, scope;
                private final int hash;

                public Label(String label, @Nullable String scope) {
                    this.label = label;
                    this.scope = scope;
                    this.hash = Objects.hash(this.label, this.scope);
                }

                @Override
                public boolean isLabel() { return true; }

                @Override
                public Label asLabel() { return this; }

                @Override
                public String toString() {
                    return String.format("Type Property: Label { label: %s, scope: %s }", label, scope);
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

            public static class Abstract extends Type {

                private final int hash;

                public Abstract() {
                    this.hash = Objects.hash(getClass());
                }

                @Override
                public boolean isAbstract() { return true; }

                @Override
                public Abstract asAbstract() { return this; }

                @Override
                public String toString() {
                    return "Type Property: Abstract";
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

            public static class ValueType extends Type {

                private final Encoding.ValueType valueType;
                private final int hash;

                public ValueType(Encoding.ValueType valueType) {
                    this.valueType = valueType;
                    this.hash = Objects.hash(this.valueType);
                }

                @Override
                public boolean isValueType() { return true; }

                @Override
                public ValueType asValueType() { return this; }

                @Override
                public String toString() {
                    return String.format("Type Property: Value Type { valueType: %s }", valueType);
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

            public static class Regex extends Type {

                private final String regex;
                private final int hash;

                public Regex(String regex) {
                    this.regex = regex;
                    this.hash = Objects.hash(this.regex);
                }

                @Override
                public boolean isRegex() { return true; }

                @Override
                public Regex asRegex() { return this; }

                @Override
                public String toString() {
                    return String.format("Type Property: Regex { regex: %s }", regex);
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
        }
    }
}
