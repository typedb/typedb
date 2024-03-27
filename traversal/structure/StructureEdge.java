/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.traversal.structure;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.traversal.graph.TraversalEdge;
import com.vaticle.typeql.lang.common.TypeQLToken;
import com.vaticle.typeql.lang.common.TypeQLToken.Annotation;

import java.util.Objects;
import java.util.Set;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static com.vaticle.typedb.core.encoding.Encoding.Edge.Thing.Optimised.ROLEPLAYER;

public abstract class StructureEdge<VERTEX_FROM extends StructureVertex<?>, VERTEX_TO extends StructureVertex<?>>
        extends TraversalEdge<VERTEX_FROM, VERTEX_TO> {

    StructureEdge(VERTEX_FROM from, VERTEX_TO to, String symbol) {
        super(from, to, symbol);
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

    public boolean isArgument() {
        return false;
    }

    public Equal asEqual() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Equal.class));
    }

    public Predicate asPredicate() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Predicate.class));
    }

    public Native<?, ?> asNative() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Native.class));
    }

    public Argument asArgument() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Argument.class));
    }

    public static class Equal extends StructureEdge<StructureVertex<?>, StructureVertex<?>> {

        private final int hash;

        Equal(StructureVertex<?> from, StructureVertex<?> to) {
            super(from, to, TypeQLToken.Predicate.Equality.EQ.toString());
            this.hash = Objects.hash(getClass(), from, to);
        }

        @Override
        public boolean isEqual() {
            return true;
        }

        @Override
        public Equal asEqual() {
            return this;
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

    public static class Predicate extends StructureEdge<StructureVertex<?>, StructureVertex<?>> {

        private final com.vaticle.typedb.core.traversal.predicate.Predicate.Variable predicate;
        private final int hash;

        Predicate(StructureVertex<?> from, StructureVertex<?> to, com.vaticle.typedb.core.traversal.predicate.Predicate.Variable predicate) {
            super(from, to, predicate.toString());
            assert (from.isThing() || from.isValue()) && (to.isThing() || to.isValue());
            this.predicate = predicate;
            this.hash = Objects.hash(getClass(), from, to, this.predicate);
        }

        public com.vaticle.typedb.core.traversal.predicate.Predicate.Variable predicate() {
            return predicate;
        }

        @Override
        public boolean isPredicate() {
            return true;
        }

        @Override
        public Predicate asPredicate() {
            return this;
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
        private final Set<Annotation> annotations;
        private final int hash;

        public Native(VERTEX_FROM from, VERTEX_TO to, Encoding.Edge encoding, boolean isTransitive, Set<Annotation> annotations) {
            super(from, to, encoding.name());
            this.encoding = encoding;
            this.isTransitive = isTransitive;
            this.annotations = annotations;
            this.hash = Objects.hash(getClass(), from, to, this.encoding, this.isTransitive, this.annotations);
        }

        public Encoding.Edge encoding() {
            return encoding;
        }

        public boolean isTransitive() {
            return isTransitive;
        }

        public Set<Annotation> annotations() {
            return this.annotations;
        }

        @Override
        public boolean isNative() {
            return true;
        }

        @Override
        public Native<?, ?> asNative() {
            return this;
        }

        public boolean isRolePlayer() {
            return false;
        }

        public RolePlayer asRolePlayer() {
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(RolePlayer.class));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Native<?, ?> that = (Native<?, ?>) o;
            return (this.from.equals(that.from) &&
                    this.to.equals(that.to) &&
                    this.encoding.equals(that.encoding) &&
                    this.annotations.equals(that.annotations) &&
                    this.isTransitive == that.isTransitive);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        public static class RolePlayer extends Native<StructureVertex.Thing, StructureVertex.Thing> {

            private final Set<Label> roleTypes;
            private final int repetition;
            private final int hash;

            RolePlayer(StructureVertex.Thing from, StructureVertex.Thing to, Set<Label> roleTypes, int repetition,
                       Set<Annotation> annotations) {
                super(from, to, ROLEPLAYER, false, annotations);
                this.roleTypes = roleTypes;
                this.repetition = repetition;
                this.hash = Objects.hash(this.getClass(), from, to, encoding, roleTypes, repetition);
            }

            public Set<Label> types() {
                return roleTypes;
            }

            @Override
            public boolean isRolePlayer() {
                return true;
            }

            @Override
            public RolePlayer asRolePlayer() {
                return this;
            }

            public int repetition() {
                return repetition;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                RolePlayer that = (RolePlayer) o;
                return (this.from.equals(that.from) &&
                        this.to.equals(that.to) &&
                        this.encoding.equals(that.encoding) &&
                        this.roleTypes.equals(that.roleTypes) &&
                        this.repetition == that.repetition);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }
    }

    public static class Argument extends StructureEdge<StructureVertex<?>, StructureVertex.Value> {

        private final int hash;

        Argument(StructureVertex<?> from, StructureVertex.Value to) {
            super(from, to, "arg");
            this.hash = Objects.hash(getClass(), from, to);
        }

        @Override
        public boolean isArgument() {
            return true;
        }

        @Override
        public Argument asArgument() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Argument that = (Argument) o;
            return (this.from.equals(that.from) && this.to.equals(that.to));
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
