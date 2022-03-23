/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.traversal.common;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typeql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.util.Objects;

import static com.vaticle.typedb.common.util.Objects.className;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class Identifier {

    public boolean isScoped() {
        return false;
    }

    public boolean isVariable() {
        return false;
    }

    public boolean isRetrievable() {
        return false;
    }

    public boolean isName() {
        return false;
    }

    public boolean isAnonymous() {
        return false;
    }

    public boolean isLabel() {
        return false;
    }

    public Scoped asScoped() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Scoped.class));
    }

    public Variable asVariable() {
        throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Variable.class));
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
        public boolean isScoped() {
            return true;
        }

        @Override
        public Scoped asScoped() {
            return this;
        }

        @Override
        public String toString() {
            return String.format("%s:%s:%s:%s", relation, roleType, player, repetition);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            else if (o == null || getClass() != o.getClass()) return false;

            Scoped that = (Scoped) o;
            return this.relation.equals(that.relation) && this.repetition == that.repetition;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public abstract static class Variable extends Identifier {

        final Reference reference;
        private final int hash;
        protected final Integer id;

        private Variable(Reference reference, @Nullable Integer id) {
            this.reference = reference;
            this.id = id;
            this.hash = Objects.hash(Variable.class, this.reference, this.id);
        }

        public static Variable of(Reference.Referable reference) {
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

        public static Name name(String name) {
            return of(Reference.name(name));
        }

        public static Label label(String label) {
            return Variable.of(Reference.label(label));
        }

        public static Anonymous anon(int id) {
            return Variable.of(Reference.anonymous(false), id);
        }

        public Reference reference() {
            return reference;
        }

        @Override
        public boolean isVariable() {
            return true;
        }

        @Override
        public Variable asVariable() {
            return this;
        }

        public Retrievable asRetrievable() {
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Retrievable.class));
        }

        public Variable.Name asName() {
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Variable.Name.class));
        }

        public Variable.Anonymous asAnonymous() {
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Variable.Anonymous.class));
        }

        public Variable.Label asLabel() {
            throw TypeDBException.of(ILLEGAL_CAST, className(this.getClass()), className(Variable.Label.class));
        }

        @Override
        public String toString() {
            return reference.syntax() + (id == null ? "" : id.toString());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            else if (o == null || getClass() != o.getClass()) return false;

            Variable that = (Variable) o;
            return this.reference.equals(that.reference) && Objects.equals(this.id, that.id);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        public static abstract class Retrievable extends Variable {

            Retrievable(Reference reference, Integer id) {
                super(reference, id);
            }

            public abstract String name();

            @Override
            public boolean isRetrievable() {
                return true;
            }

            @Override
            public Retrievable asRetrievable() {
                return this;
            }
        }

        public static class Name extends Retrievable {

            private Name(Reference.Name reference) {
                super(reference, null);
            }

            @Override
            public String name() {
                return reference.asName().name();
            }

            @Override
            public Reference.Name reference() {
                return reference.asName();
            }

            @Override
            public boolean isName() {
                return true;
            }

            @Override
            public Variable.Name asName() {
                return this;
            }
        }

        public static class Anonymous extends Retrievable {

            private Anonymous(Reference.Anonymous reference, int id) {
                super(reference, id);
            }

            public String name() {
                return reference().name() + id;
            }

            public int anonymousId() {
                return id;
            }

            @Override
            public Reference.Anonymous reference() {
                return reference.asAnonymous();
            }

            @Override
            public boolean isAnonymous() {
                return true;
            }

            @Override
            public Variable.Anonymous asAnonymous() {
                return this;
            }
        }

        public static class Label extends Variable {

            private Label(Reference.Label reference) {
                super(reference, null);
            }

            @Override
            public Reference.Label reference() {
                return reference.asLabel();
            }

            @Override
            public boolean isLabel() {
                return true;
            }

            @Override
            public Variable.Label asLabel() {
                return this;
            }
        }
    }
}
