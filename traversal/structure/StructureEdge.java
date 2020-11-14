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
import graql.lang.common.GraqlToken;

import java.util.Arrays;
import java.util.Objects;

public class StructureEdge {

    private final Property property;
    private final StructureVertex from;
    private final StructureVertex to;
    private final int hash;

    StructureEdge(Property property, StructureVertex from, StructureVertex to) {
        this.property = property;
        this.from = from;
        this.to = to;
        this.hash = Objects.hash(this.property, this.from, this.to);
    }

    public Property property() {
        return property;
    }

    public StructureVertex from() {
        return from;
    }

    public StructureVertex to() {
        return to;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        StructureEdge that = (StructureEdge) object;
        return (this.property.equals(that.property) &&
                this.from.equals(that.from) &&
                this.to.equals(that.to));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static class Property {

        boolean isEqual() {
            return false;
        }

        boolean isComparator() {
            return false;
        }

        boolean isProperty() {
            return false;
        }

        static class Equal extends Property {

            @Override
            boolean isEqual() {
                return true;
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

        static class Comparator extends Property {

            private final GraqlToken.Comparator.Equality comparator;
            private final int hash;

            Comparator(GraqlToken.Comparator.Equality comparator) {
                this.comparator = comparator;
                this.hash = Objects.hash(this.comparator);
            }

            GraqlToken.Comparator.Equality comparator() {
                return comparator;
            }

            @Override
            boolean isComparator() {
                return false;
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

        static class Type extends Property {

            private final Encoding.Edge encoding;
            private final String[] labels;
            private final boolean isTransitive;
            private final int hash;

            Type(Encoding.Edge encoding) {
                this(encoding, new String[]{}, false);
            }

            Type(Encoding.Edge encoding, boolean isTransitive) {
                this(encoding, new String[]{}, isTransitive);
            }

            Type(Encoding.Edge encoding, String[] labels) {
                this(encoding, labels, false);
            }

            private Type(Encoding.Edge encoding, String[] labels, boolean isTransitive) {
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
            boolean isProperty() {
                return true;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Type that = (Type) o;
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
