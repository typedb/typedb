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

package grakn.core.pattern.constraint.thing;

import grakn.core.pattern.variable.ThingVariable;
import grakn.core.pattern.variable.TypeVariable;
import grakn.core.pattern.variable.Variable;
import grakn.core.pattern.variable.VariableRegistry;
import grakn.core.traversal.Traversal;

import java.util.Objects;
import java.util.Set;

import static grakn.common.collection.Collections.set;

public class IsaConstraint extends ThingConstraint {

    private final TypeVariable type;
    private final boolean isExplicit;
    private final int hash;
    private String[] labels;

    private IsaConstraint(final ThingVariable owner, final TypeVariable type, final boolean isExplicit) {
        super(owner);
        this.type = type;
        this.isExplicit = isExplicit;
        this.hash = Objects.hash(IsaConstraint.class, this.owner, this.type, this.isExplicit);
    }

    public static IsaConstraint of(ThingVariable owner, graql.lang.pattern.constraint.ThingConstraint.Isa constraint,
                                   VariableRegistry registry) {
        return new IsaConstraint(owner, registry.register(constraint.type()), constraint.isExplicit());
    }

    public TypeVariable type() {
        return type;
    }

    public boolean isExplicit() {
        return isExplicit;
    }

    public void setLabels(String[] labels) {
        this.labels = labels;
    }

    @Override
    public Set<Variable> variables() {
        return set(type);
    }

    @Override
    public void addTo(final Traversal traversal) {
        if (!type.reference().isName() && labels != null && labels.length > 0) {
            traversal.type(owner.identifier(), labels);
        } else {
            traversal.isa(owner.identifier(), type.identifier(), !isExplicit);
        }
    }

    @Override
    public boolean isIsa() {
        return true;
    }

    @Override
    public IsaConstraint asIsa() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final IsaConstraint that = (IsaConstraint) o;
        return (this.owner.equals(that.owner) &&
                this.type.equals(that.type) &&
                this.isExplicit == that.isExplicit);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
