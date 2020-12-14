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

package grakn.core.pattern.variable;

import grakn.core.common.exception.GraknException;
import grakn.core.pattern.Pattern;
import grakn.core.pattern.constraint.Constraint;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.variable.Reference;

import java.util.Objects;
import java.util.Set;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class Variable implements Pattern {

    private final Identifier.Variable identifier;
    private final int hash;

    Variable(Identifier.Variable identifier) {
        this.identifier = identifier;
        this.hash = Objects.hash(identifier);
    }

    public abstract Set<? extends Constraint> constraints();

    public Identifier.Variable identifier() {
        return identifier;
    }

    public Reference reference() {
        return identifier.reference();
    }

    public void addTo(Traversal traversal) {
        // TODO: create vertex properties first, then the vertex itself, then edges
        //       that way, we can make properties to be 'final' objects that are
        //       included in equality and hashCode of vertices
        constraints().forEach(constraint -> constraint.addTo(traversal));
    }

    public boolean isType() {
        return false;
    }

    public boolean isThing() {
        return false;
    }

    public TypeVariable asType() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(TypeVariable.class));
    }

    public ThingVariable asThing() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(ThingVariable.class));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Variable that = (Variable) o;
        return this.identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
