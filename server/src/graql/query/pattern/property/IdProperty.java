/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.query.pattern.property;

import grakn.core.graql.query.Query;

/**
 * Represents the {@code id} property on a Concept.
 * This property can be queried. While this property cannot be inserted, if used in an insert query any existing concept
 * with the given ID will be retrieved.
 */
public class IdProperty extends VarProperty {

    private final String id;

    public IdProperty(String id) {
        if (id == null) {
            throw new NullPointerException("Null id");
        }
        this.id = id;
    }

    public String id() {
        return id;
    }

    @Override
    public String keyword() {
        return Query.Property.ID.toString();
    }

    @Override
    public String property() {
        return id;
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public boolean uniquelyIdentifiesConcept() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof IdProperty) {
            IdProperty that = (IdProperty) o;
            return (this.id.equals(that.id()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.id.hashCode();
        return h;
    }
}
