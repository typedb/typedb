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

public class EdgeProperty {

    boolean isEqual() {
        return false;
    }

    boolean isComparator() {
        return false;
    }

    boolean isProperty() {
        return false;
    }

    public static class Equal extends EdgeProperty {

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

    public static class Comparator extends EdgeProperty {

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

    public static class Type extends EdgeProperty {

        private final Encoding.Edge encoding;
        private final String[] labels;
        private final boolean isTransitive;
        private final int hash;

        public Type(Encoding.Edge encoding) {
            this(encoding, new String[]{}, false);
        }

        public Type(Encoding.Edge encoding, boolean isTransitive) {
            this(encoding, new String[]{}, isTransitive);
        }

        public Type(Encoding.Edge encoding, String[] labels) {
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
