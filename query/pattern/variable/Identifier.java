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

package grakn.core.query.pattern.variable;

import graql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.util.Objects;

public abstract class Identifier {

    final Reference reference;
    private final Integer id;
    private final int hash;

    private Identifier(Reference reference, @Nullable Integer id) {
        this.reference = reference;
        this.id = id;
        this.hash = Objects.hash(this.reference, this.id);
    }

    static Identifier.Referrable of(Reference.Referrable reference) {
        if (reference.isLabel()) return Identifier.of(reference.asLabel());
        else if (reference.isName()) return Identifier.of(reference.asName());
        else assert false;
        return null;
    }

    static Identifier.Name of(Reference.Name reference) {
        return new Identifier.Name(reference);
    }

    static Identifier.Label of(Reference.Label reference) {
        return new Identifier.Label(reference);
    }

    static Identifier.Anonymous of(Reference.Anonymous reference, int id) {
        return new Identifier.Anonymous(reference, id);
    }

    public Reference reference() {
        return reference;
    }

    @Override
    public String toString() {
        return reference.identifier() + (id == null ? "" : id.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        else if (o == null || getClass() != o.getClass()) return false;

        Identifier that = (Identifier) o;
        return this.reference.equals(that.reference) && Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    static class Referrable extends Identifier {

        Referrable(Reference reference) {
            super(reference, null);
        }

        @Override
        public Reference.Referrable reference() {
            return reference.asReferrable();
        }
    }

    static class Name extends Identifier.Referrable {

        private Name(Reference.Name reference) {
            super(reference);
        }

        @Override
        public Reference.Name reference() {
            return reference.asName();
        }
    }

    static class Label extends Identifier.Referrable {

        private Label(Reference.Label reference) {
            super(reference);
        }

        @Override
        public Reference.Label reference() {
            return reference.asLabel();
        }
    }

    static class Anonymous extends Identifier {

        private Anonymous(Reference.Anonymous reference, int id) {
            super(reference, id);
        }

        @Override
        public Reference.Anonymous reference() {
            return reference.asAnonymous();
        }
    }
}
