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

import io.mindmaps.concept.Concept;
import io.mindmaps.graql.admin.UniqueVarProperty;
import io.mindmaps.graql.internal.gremlin.fragment.Fragment;
import io.mindmaps.graql.internal.gremlin.fragment.Fragments;
import io.mindmaps.graql.internal.query.InsertQueryExecutor;
import io.mindmaps.graql.internal.util.StringConverter;

import static io.mindmaps.util.ErrorMessage.INSERT_RESOURCE_WITH_ID;

public class IdProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty, SingleFragmentProperty {

    private final String id;

    public IdProperty(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return "id";
    }

    @Override
    public String getProperty() {
        return StringConverter.valueToString(id);
    }

    @Override
    public Fragment getFragment(String start) {
        return Fragments.id(start, id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdProperty that = (IdProperty) o;

        return id.equals(that.id);

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        if (concept.isResource()) {
            throw new IllegalStateException(INSERT_RESOURCE_WITH_ID.getMessage(id));
        }
    }
}
