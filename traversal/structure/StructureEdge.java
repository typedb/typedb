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
import grakn.core.common.parameters.Label;
import grakn.core.graph.util.Encoding;
import grakn.core.traversal.graph.TraversalEdge;

import java.util.Objects;
import java.util.Set;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class StructureEdge<VERTEX_FROM extends StructureVertex<?>, VERTEX_TO extends StructureVertex<?>>
        extends TraversalEdge<VERTEX_FROM, VERTEX_TO> {

    StructureEdge(VERTEX_FROM from, VERTEX_TO to) {
        super(from, to);
    }

    public boolean isEqual() {
        return false;
    }

    public boolean isPredicate() {
        return false;
    }

    public boolean isNative() {
        return false;
    }

    public Equal asEqual() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Equal.class));
    }

    public Predicate asPredicate() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Predicate.class));
    }

    public Native<?, ?> asNative() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Native.class));
    }

    @Override
    public abstract String toString();

    public static class Equal extends StructureEdge<StructureVertex<?>, StructureVertex<?>> {

        private final int hash;

        Equal(StructureVertex<?> from, StructureVertex<?> to) {
            super(from, to);
            this.hash = Objects.hash(getClass(), from, to);
        }

        @Override
        public boolean isEqual() { return true; }

        @Override
        public Equal asEqual() { return this; }

        @Override
        public String toString() {
            return String.format("Equal Edge (%s --> %s)", from, to);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Equal that = (Equal) o;
            return this.from.equals(that.from) && this.to.equals(that.to);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Predicate extends StructureEdge<StructureVertex.Thing, StructureVertex.Thing> {

        private final grakn.core.traversal.common.Predicate.Variable predicate;
        private final int hash;

        Predicate(StructureVertex.Thing from, StructureVertex.Thing to, grakn.core.traversal.common.Predicate.Variable predicate) {
            super(from, to);
            this.predicate = predicate;
            this.hash = Objects.hash(getClass(), from, to, this.predicate);
        }

        public grakn.core.traversal.common.Predicate.Variable predicate() {
            return predicate;
        }

        @Override
        public boolean isPredicate() { return true; }

        @Override
        public Predicate asPredicate() { return this; }

        @Override
        public String toString() {
            return String.format("Predicate Edge (%s --> %s) { predicate: %s }", from, to, predicate);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Predicate that = (Predicate) o;
            return (this.from.equals(that.from) &&
                    this.to.equals(that.to) &&
                    this.predicate.equals(that.predicate));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static class Native<VERTEX_FROM extends StructureVertex<?>, VERTEX_TO extends StructureVertex<?>>
            extends StructureEdge<VERTEX_FROM, VERTEX_TO> {

        protected final Encoding.Edge encoding;
        private final boolean isTransitive;
        private final int hash;

        public Native(VERTEX_FROM from, VERTEX_TO to, Encoding.Edge encoding, boolean isTransitive) {
            super(from, to);
            this.encoding = encoding;
            this.isTransitive = isTransitive;
            this.hash = Objects.hash(getClass(), from, to, this.encoding, this.isTransitive);
        }

        public Encoding.Edge encoding() {
            return encoding;
        }

        public boolean isTransitive() {
            return isTransitive;
        }

        public boolean isOptimised() {
            return false;
        }

        public Native.Optimised asOptimised() {
            throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Native.Optimised.class));
        }

        @Override
        public boolean isNative() { return true; }

        @Override
        public Native<?, ?> asNative() { return this; }

        @Override
        public String toString() {
            return String.format("Native Edge (%s --> %s) { encoding: %s, isTransitive: %s }",
                                 from, to, encoding, isTransitive);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Native<?, ?> that = (Native<?, ?>) o;
            return (this.from.equals(that.from) &&
                    this.to.equals(that.to) &&
                    this.encoding.equals(that.encoding) &&
                    this.isTransitive == that.isTransitive);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        public static class Optimised extends Native<StructureVertex.Thing, StructureVertex.Thing> {

            private final Set<Label> types;
            private final int hash;

            Optimised(StructureVertex.Thing from, StructureVertex.Thing to, Encoding.Edge encoding, Set<Label> types) {
                super(from, to, encoding, false);
                this.types = types;
                this.hash = Objects.hash(this.getClass(), from, to, encoding, types);
            }

            public Set<Label> types() {
                return types;
            }

            @Override
            public boolean isOptimised() { return true; }

            @Override
            public Optimised asOptimised() { return this; }

            @Override
            public String toString() {
                return String.format("Optimised Edge (%s --> %s) { labels: %s }", from, to, types);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Optimised that = (Optimised) o;
                return (this.from.equals(that.from) &&
                        this.to.equals(that.to) &&
                        this.encoding.equals(that.encoding) &&
                        this.types.equals(that.types));
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }
    }
}
