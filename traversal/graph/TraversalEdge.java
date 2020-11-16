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

import grakn.core.graph.util.Encoding;
import graql.lang.common.GraqlToken;

import java.util.Arrays;
import java.util.Objects;

public abstract class TraversalEdge<VERTEX extends TraversalVertex<?, ?>> {

    private final Property property;
    private final VERTEX from;
    private final VERTEX to;
    private final int hash;

    public TraversalEdge(Property property, VERTEX from, VERTEX to) {
        this.property = property;
        this.from = from;
        this.to = to;
        this.hash = Objects.hash(property, from, to);
    }

    public Property property() {
        return property;
    }

    public VERTEX from() {
        return from;
    }

    public VERTEX to() {
        return to;
    }

    @Override
    public String toString() {
        return String.format("(%s --> %s), %s", from, to, property);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        TraversalEdge<?> that = (TraversalEdge<?>) object;
        return (this.property.equals(that.property) &&
                this.from.equals(that.from) &&
                this.to.equals(that.to));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static abstract class Property {

        boolean isEqual() {
            return false;
        }

        boolean isComparator() {
            return false;
        }

        boolean isEncoder() {
            return false;
        }

        @Override
        public abstract String toString();

        public static class Equal extends Property {

            @Override
            boolean isEqual() {
                return true;
            }

            @Override
            public String toString() {
                return "Property: Equal";
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                return o != null && getClass() == o.getClass();
            }

            @Override
            public int hashCode() {
                return Objects.hash(getClass());
            }
        }

        public static class Comparator extends Property {

            private final GraqlToken.Comparator.Equality comparator;
            private final int hash;

            public Comparator(GraqlToken.Comparator.Equality comparator) {
                this.comparator = comparator;
                this.hash = Objects.hash(this.comparator);
            }

            GraqlToken.Comparator.Equality comparator() {
                return comparator;
            }

            @Override
            boolean isComparator() {
                return true;
            }

            @Override
            public String toString() {
                return String.format("Property: Comparator { comparator: %s }", comparator);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Comparator that = (Comparator) o;
                return this.comparator.equals(that.comparator);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        public static class Encoder extends Property {

            private final Encoding.Edge encoding;
            private final String[] labels;
            private final boolean isTransitive;
            private final int hash;

            public Encoder(Encoding.Edge encoding) {
                this(encoding, new String[]{}, false);
            }

            public Encoder(Encoding.Edge encoding, boolean isTransitive) {
                this(encoding, new String[]{}, isTransitive);
            }

            public Encoder(Encoding.Edge encoding, String[] labels) {
                this(encoding, labels, false);
            }

            private Encoder(Encoding.Edge encoding, String[] labels, boolean isTransitive) {
                this.encoding = encoding;
                this.labels = labels;
                this.isTransitive = isTransitive;
                this.hash = Objects.hash(this.encoding, Arrays.hashCode(this.labels), this.isTransitive);
            }

            public Encoding.Edge encoding() {
                return encoding;
            }

            public String[] labels() {
                return labels;
            }

            public boolean isTransitive() {
                return isTransitive;
            }

            @Override
            boolean isEncoder() {
                return true;
            }

            @Override
            public String toString() {
                return String.format("Property: Encoder { encoding: %s, labels: %s, isTransitive: %s }",
                                     encoding(), Arrays.toString(labels), isTransitive);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Encoder that = (Encoder) o;
                return (this.encoding.equals(that.encoding) &&
                        Arrays.equals(this.labels, that.labels) &&
                        this.isTransitive == that.isTransitive);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }
    }
}
