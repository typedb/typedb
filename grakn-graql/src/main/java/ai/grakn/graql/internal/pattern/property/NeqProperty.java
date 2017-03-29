/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.reasoner.atom.NotEquals;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Represents the {@code !=} property on a {@link ai.grakn.concept.Concept}.
 *
 * This property can be queried. It asserts identity inequality between two concepts. Concepts may have shared
 * properties but still be distinct. For example, two instances of a type without any resources are still considered
 * unequal. Similarly, two resources with the same value but of different types are considered unequal.
 *
 * @author Felix Chapman
 */
public class NeqProperty extends AbstractVarProperty implements NamedProperty {

    private final VarAdmin var;

    public NeqProperty(VarAdmin var) {
        this.var = var;
    }

    public VarAdmin getVar() {
        return var;
    }

    @Override
    public String getName() {
        return "!=";
    }

    @Override
    public String getProperty() {
        return var.getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(VarName start) {
        return Sets.newHashSet(
                EquivalentFragmentSets.notCasting(start),
                EquivalentFragmentSets.notCasting(var.getVarName()),
                EquivalentFragmentSets.neq(start, var.getVarName())
        );
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(var);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NeqProperty that = (NeqProperty) o;

        return var.equals(that.var);
    }

    @Override
    public int hashCode() {
        return var.hashCode();
    }

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        return new NotEquals(var.getVarName(), this, parent);
    }
}
