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
import graql.lang.pattern.variable.Reference;

import java.util.List;
import java.util.Set;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static java.util.stream.Collectors.toList;

public abstract class Variable implements Pattern {

    final Identifier identifier;

    Variable(final Identifier identifier) {
        this.identifier = identifier;
    }

    public abstract Set<? extends Constraint> constraints();

    public Identifier identifier() {
        return identifier;
    }

    public Reference reference() {
        return identifier.reference();
    }

    public List<Traversal> traversals() {
        return constraints().stream().flatMap(c -> c.traversals().stream()).collect(toList());
    }

    public boolean isType() {
        return false;
    }

    public boolean isThing() {
        return false;
    }

    public TypeVariable asType() {
        throw new GraknException(INVALID_CASTING.message(className(this.getClass()), className(TypeVariable.class)));
    }

    public ThingVariable asThing() {
        throw new GraknException(INVALID_CASTING.message(className(this.getClass()), className(ThingVariable.class)));
    }

    @Override
    public abstract boolean equals(Object o);

    @Override
    public abstract int hashCode();
}
