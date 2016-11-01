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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;

public class LhsProperty extends AbstractVarProperty implements UniqueVarProperty, NamedProperty, SingleFragmentProperty {

    private final String lhs;

    public LhsProperty(String lhs) {
        this.lhs = lhs;
    }

    public String getLhs() {
        return lhs;
    }

    @Override
    public String getName() {
        return "lhs";
    }

    @Override
    public String getProperty() {
        return "{" + lhs + "}";
    }

    @Override
    public Fragment getFragment(String start) {
        return Fragments.lhs(start, lhs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LhsProperty that = (LhsProperty) o;

        return lhs.equals(that.lhs);

    }

    @Override
    public int hashCode() {
        return lhs.hashCode();
    }
}
