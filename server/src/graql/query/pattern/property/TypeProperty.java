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
import grakn.core.graql.util.StringUtil;

/**
 * Represents the {@code label} property on a Type.
 * This property can be queried and inserted. If used in an insert query and there is an existing type with the give
 * label, then that type will be retrieved.
 */
public class TypeProperty extends VarProperty {

    private final String name;

    public TypeProperty(String name) {
        if (name == null) {
            throw new NullPointerException("Null label");
        }
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public String keyword() {
        return Query.Property.TYPE.toString();
    }

    @Override
    public String property() {
        return StringUtil.typeLabelToString(name());
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
        if (o instanceof TypeProperty) {
            TypeProperty that = (TypeProperty) o;
            return (this.name.equals(that.name));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.name.hashCode();
        return h;
    }
}
