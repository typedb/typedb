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

package grakn.core.pattern.constraint.type;

import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;
import grakn.core.traversal.TraversalVertex;
import graql.lang.pattern.constraint.ConceptConstraint;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.set;

public class IsConstraint extends TypeConstraint {

    private final TypeVariable variable;
    private final int hash;
    private List<Traversal> traversals;

    private IsConstraint(final TypeVariable owner, final TypeVariable variable) {
        super(owner);
        this.variable = variable;
        this.hash = Objects.hash(IsConstraint.class, this.owner, this.variable);
    }

    public static IsConstraint of(final TypeVariable owner,
                                  final ConceptConstraint.Is constraint,
                                  final VariableRegistry registry) {
        return new IsConstraint(owner, registry.register(constraint.variable()).asType());
    }

    public TypeVariable variable() {
        return variable;
    }

    @Override
    public Set<TypeVariable> variables() {
        return set(variable());
    }

    @Override
    public void addTo(final Traversal traversal) {
        traversal.is(owner.identifier(), variable.identifier());
    }

    @Override
    public boolean isIs() {
        return true;
    }

    @Override
    public IsConstraint asIs() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final IsConstraint that = (IsConstraint) o;
        return (this.owner.equals(that.owner) && this.variable.equals(that.variable));
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
