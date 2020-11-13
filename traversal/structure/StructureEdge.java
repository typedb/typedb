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

class StructureEdge {

    private final Type type;
    private final StructureVertex from;
    private final StructureVertex to;
    private final boolean isTransitive;
    private final String[] labels;
    private final int hash;

    StructureEdge(Type type, StructureVertex from, StructureVertex to,
                  boolean isTransitive, String[] labels) {
        this.type = type;
        this.from = from;
        this.to = to;
        this.isTransitive = isTransitive;
        this.labels = labels;
        this.hash = Objects.hash(this.type, from, to, isTransitive, Arrays.hashCode(labels));
    }

    StructureVertex from() {
        return from;
    }

    StructureVertex to() {
        return to;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        final StructureEdge that = (StructureEdge) object;
        return (this.type.equals(that.type) &&
                this.from.equals(that.from) &&
                this.to.equals(that.to) &&
                this.isTransitive == that.isTransitive &&
                Arrays.equals(this.labels, that.labels));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    static class Type {

        boolean isEqual() {
            return false;
        }

        boolean isComparator() {
            return false;
        }

        boolean isEncoded() {
            return false;
        }

        static class Equal extends Type {

            @Override
            boolean isEqual() {
                return true;
            }
        }

        static class Comparator extends Type {

            private final GraqlToken.Comparator.Equality comparator;

            Comparator(GraqlToken.Comparator.Equality comparator) {
                this.comparator = comparator;
            }

            GraqlToken.Comparator.Equality comparator() {
                return comparator;
            }

            @Override
            boolean isComparator() {
                return false;
            }
        }

        static class Encoded extends Type {

            private final Encoding.Edge encoding;

            Encoded(Encoding.Edge encoding) {
                this.encoding = encoding;
            }

            Encoding.Edge encoding() {
                return encoding;
            }

            @Override
            boolean isEncoded() {
                return true;
            }
        }
    }
}
