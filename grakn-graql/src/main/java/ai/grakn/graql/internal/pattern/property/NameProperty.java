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

import ai.grakn.concept.TypeName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.util.StringConverter;
import java.util.Set;

public class NameProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty, SingleFragmentProperty {

    private final TypeName name;

    public NameProperty(TypeName name) {
        this.name = name;
    }

    public TypeName getNameValue() {
        return name;
    }

    @Override
    public String getName() {
        return "type-name";
    }

    @Override
    public String getProperty() {
        return StringConverter.idToString(name.getValue());
    }

    @Override
    public Fragment getFragment(VarName start) {
        return Fragments.name(start, name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NameProperty that = (NameProperty) o;

        return name.equals(that.name);

    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        return new IdPredicate(var.getVarName(), this, parent);
    }
}
