/*
 * Copyright (C) 2021 Grakn Labs
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

    public boolean isScoped() { return false; }

    public boolean isVariable() { return false; }

    public boolean isName() { return false; }

    public boolean isLabel() { return false; }

    public boolean isAnonymous() { return false; }

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

        public static Referable of(Reference.Referable reference) {
            if (reference.isLabel()) return of(reference.asLabel());
            else if (reference.isName()) return of(reference.asName());
            else assert false;
            return null;
        }

        public static Variable.Label of(Reference.Label reference) {
            return new Label(reference);
        }

        public static Variable.Name of(Reference.Name reference) {
            return new Name(reference);
        }

        public static Anonymous of(Reference.Anonymous reference, int id) {
            return new Anonymous(reference, id);
        }

        public static Referable name(String name) {
            return Variable.of(Reference.name(name));
        }

        public static Referable label(String label) {
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

        public Variable.Name asName() {
            throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Variable.Name.class));
        }

        public Variable.Label asLabel() {
            throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Variable.Label.class));
        }

        public Variable.Anonymous asAnonymous() {
            throw GraknException.of(ILLEGAL_CAST, className(this.getClass()), className(Variable.Anonymous.class));
        }

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

        public static class Referable extends Variable {

            Referable(Reference reference) {
                super(reference, null);
            }

            @Override
            public Reference.Referable reference() {
                return reference.asReferable();
            }
        }

        public static class Name extends Referable {

            private Name(Reference.Name reference) {
                super(reference);
            }

            @Override
            public Reference.Name reference() {
                return reference.asName();
            }

            @Override
            public boolean isName() { return true; }

            @Override
            public Variable.Name asName() { return this; }
        }

        public static class Label extends Referable {

            private Label(Reference.Label reference) {
                super(reference);
            }

            @Override
            public Reference.Label reference() {
                return reference.asLabel();
            }

            @Override
            public boolean isLabel() { return true; }

            @Override
            public Variable.Label asLabel() { return this; }
        }

        public static class Anonymous extends Variable {

            private Anonymous(Reference.Anonymous reference, int id) {
                super(reference, id);
            }

            @Override
            public Reference.Anonymous reference() {
                return reference.asAnonymous();
            }

            @Override
            public boolean isAnonymous() { return true; }

            @Override
            public Variable.Anonymous asAnonymous() { return this; }
        }
    }
}
