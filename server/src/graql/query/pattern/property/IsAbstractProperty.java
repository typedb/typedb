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
 * Represents the {@code is-abstract} property on a Type.
 * This property can be matched or inserted.
 * This property states that a type cannot have direct instances.
 */
public class IsAbstractProperty extends VarProperty {

    private static final IsAbstractProperty INSTANCE = new IsAbstractProperty();

    private IsAbstractProperty() {}

    public static IsAbstractProperty get() {
        return INSTANCE;
    }

    @Override
    public String name() {
        return Query.Property.IS_ABSTRACT.toString();
    }

    @Override
    public String property() {
        return null;
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public String toString() {
        return name();
    }
}
