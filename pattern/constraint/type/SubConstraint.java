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

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.set;

public class SubConstraint extends TypeConstraint {

    private final TypeVariable type;
    private final boolean isExplicit;
    private final int hash;
    private List<Traversal> traversals;

    private SubConstraint(final TypeVariable owner, final TypeVariable type, final boolean isExplicit) {
        super(owner);
        if (type == null) throw new NullPointerException("Null superType");
        this.type = type;
        this.isExplicit = isExplicit;
        this.hash = Objects.hash(SubConstraint.class, this.owner, this.type, this.isExplicit);
    }

    public static SubConstraint of(final TypeVariable owner,
                                   final graql.lang.pattern.constraint.TypeConstraint.Sub constraint,
                                   final VariableRegistry registry) {
        return new SubConstraint(owner, registry.register(constraint.type()), constraint.isExplicit());
    }

    public TypeVariable type() {
        return type;
    }

    @Override
    public Set<TypeVariable> variables() {
        return set(type);
    }

    @Override
    public void addTo(final Traversal traversal) {
        traversal.sub(owner.identifier(), type.identifier(), !isExplicit);
    }

    @Override
    public boolean isSub() {
        return true;
    }

    @Override
    public SubConstraint asSub() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SubConstraint that = (SubConstraint) o;
        return (this.owner.equals(that.owner) &&
                this.type.equals(that.type) &&
                this.isExplicit == that.isExplicit);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
