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
 */

package grakn.core.graph.graphdb.types;

import com.google.common.base.Preconditions;

public class TypeDefinitionDescription {

    private final TypeDefinitionCategory category;
    private final Object modifier;

    public TypeDefinitionDescription(TypeDefinitionCategory category, Object modifier) {
        Preconditions.checkNotNull(category);
        if (category.isProperty()) Preconditions.checkArgument(modifier==null);
        else {
            Preconditions.checkArgument(category.isEdge());
            if (category.hasDataType()) Preconditions.checkArgument(modifier==null || modifier.getClass().equals(category.getDataType()));
            else Preconditions.checkArgument(modifier==null);
        }
        this.category = category;
        this.modifier = modifier;
    }

    public static TypeDefinitionDescription of(TypeDefinitionCategory category) {
        return new TypeDefinitionDescription(category,null);
    }

    public TypeDefinitionCategory getCategory() {
        return category;
    }

    public Object getModifier() {
        return modifier;
    }
}
