/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.graql.internal.pattern.property;

import io.grakn.graql.admin.ValuePredicateAdmin;
import io.grakn.graql.admin.VarAdmin;
import io.grakn.graql.internal.gremlin.fragment.Fragment;
import io.grakn.graql.internal.gremlin.fragment.Fragments;
import io.grakn.util.ErrorMessage;

public class ValueProperty extends AbstractVarProperty implements NamedProperty, SingleFragmentProperty {

    private final ValuePredicateAdmin predicate;

    public ValueProperty(ValuePredicateAdmin predicate) {
        this.predicate = predicate;
    }

    public ValuePredicateAdmin getPredicate() {
        return predicate;
    }

    @Override
    public String getName() {
        return "value";
    }

    @Override
    public String getProperty() {
        return predicate.toString();
    }

    @Override
    public Fragment getFragment(String start) {
        return Fragments.value(start, predicate);
    }

    @Override
    public void checkInsertable(VarAdmin var) {
        if (!predicate.equalsValue().isPresent()) {
            throw new IllegalStateException(ErrorMessage.INSERT_PREDICATE.getMessage());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValueProperty that = (ValueProperty) o;

        return predicate.equals(that.predicate);

    }

    @Override
    public int hashCode() {
        return predicate.hashCode();
    }
}
