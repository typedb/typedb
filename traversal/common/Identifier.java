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

package grakn.core.traversal.common;

import grakn.core.common.exception.GraknException;
import graql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.util.Objects;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class Identifier {

    public boolean isNamedReference() {
        return isVariable() && asVariable().reference().isName();
    }

    public boolean isScoped() { return false; }

    public boolean isVariable() { return false; }

    public Scoped asScoped() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Scoped.class));
    }

    public Variable asVariable() {
        throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Variable.class));
    }

    @Override
    public abstract String toString();

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();

    public static class Scoped extends Identifier {

        private final Variable relation;
        private final Variable roleType;
        private final Variable player;
        private final int repetition;
        private final int hash;

        private Scoped(Variable relation, Variable roleType, Variable player, int repetition) {
            this.relation = relation;
            this.roleType = roleType;
            this.player = player;
            this.repetition = repetition;
            this.hash = Objects.hash(Scoped.class, relation, roleType, player, repetition);
        }

        public static Scoped of(Variable relation, Variable roleType, Variable player, int repetition) {
            return new Scoped(relation, roleType, player, repetition);
        }

        public Identifier.Variable scope() {
            return relation;
        }

        @Override
        public boolean isScoped() { return true; }

        @Override
        public Scoped asScoped() { return this; }

        @Override
        public String toString() {
            return String.format("%s:%s:%s:%s", relation, roleType, player, repetition);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            else if (o == null || getClass() != o.getClass()) return false;

            final Scoped that = (Scoped) o;
            return this.relation.equals(that.relation) && this.repetition == that.repetition;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public abstract static class Variable extends Identifier {

        final Reference reference;
        private final Integer id;
        private final int hash;

        private Variable(Reference reference, @Nullable Integer id) {
            this.reference = reference;
            this.id = id;
            this.hash = Objects.hash(Variable.class, this.reference, this.id);
        }

        public static Referrable of(Reference.Referrable reference) {
            if (reference.isLabel()) return new Label(reference.asLabel());
            else if (reference.isName()) return new Name(reference.asName());
            else assert false;
            return null;
        }

        public static Anonymous of(Reference.Anonymous reference, int id) {
            return new Anonymous(reference, id);
        }

        public static Referrable name(String name) {
            return Variable.of(Reference.named(name));
        }

        public static Referrable label(String label) {
            return Variable.of(Reference.label(label));
        }

        public static Anonymous anon(int id) {
            return Variable.of(Reference.anonymous(false), id);
        }

        public Reference reference() {
            return reference;
        }

        @Override
        public boolean isVariable() { return true; }

        @Override
        public Variable asVariable() { return this; }

        @Override
        public String toString() {
            return reference.syntax() + (id == null ? "" : id.toString());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            else if (o == null || getClass() != o.getClass()) return false;

            final Variable that = (Variable) o;
            return this.reference.equals(that.reference) && Objects.equals(this.id, that.id);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        static class Referrable extends Variable {

            Referrable(Reference reference) {
                super(reference, null);
            }

            @Override
            public Reference.Referrable reference() {
                return reference.asReferrable();
            }
        }

        static class Name extends Referrable {

            private Name(Reference.Name reference) {
                super(reference);
            }

            @Override
            public Reference.Name reference() {
                return reference.asName();
            }
        }

        static class Label extends Referrable {

            private Label(Reference.Label reference) {
                super(reference);
            }

            @Override
            public Reference.Label reference() {
                return reference.asLabel();
            }
        }

        static class Anonymous extends Variable {

            private Anonymous(Reference.Anonymous reference, int id) {
                super(reference, id);
            }

            @Override
            public Reference.Anonymous reference() {
                return reference.asAnonymous();
            }
        }
    }
}
