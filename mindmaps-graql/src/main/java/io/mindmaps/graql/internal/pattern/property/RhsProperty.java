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

package io.mindmaps.graql.internal.pattern.property;

import io.mindmaps.graql.Pattern;
import io.mindmaps.graql.admin.UniqueVarProperty;
import io.mindmaps.graql.internal.gremlin.fragment.Fragment;
import io.mindmaps.graql.internal.gremlin.fragment.Fragments;

public class RhsProperty extends AbstractVarProperty implements UniqueVarProperty, NamedProperty, SingleFragmentProperty {

    private final Pattern rhs;

    public RhsProperty(Pattern rhs) {
        this.rhs = rhs;
    }

    public Pattern getRhs() {
        return rhs;
    }

    @Override
    public String getName() {
        return "rhs";
    }

    @Override
    public String getProperty() {
        return rhs.toString();
    }

    @Override
    public Fragment getFragment(String start) {
        return Fragments.rhs(start, rhs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RhsProperty that = (RhsProperty) o;

        return rhs.equals(that.rhs);

    }

    @Override
    public int hashCode() {
        return rhs.hashCode();
    }
}
