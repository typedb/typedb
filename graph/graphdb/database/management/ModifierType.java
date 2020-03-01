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

package grakn.core.graph.graphdb.database.management;

import grakn.core.graph.graphdb.types.TypeDefinitionCategory;

public enum ModifierType {
    CONSISTENCY(TypeDefinitionCategory.CONSISTENCY_LEVEL),
    TTL(TypeDefinitionCategory.TTL);

    private final TypeDefinitionCategory category;

    ModifierType(TypeDefinitionCategory category) {
        this.category = category;
    }

    public TypeDefinitionCategory getCategory() {
        return category;
    }
}
