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

import java.util.EnumMap;
import java.util.Set;

public class TypeDefinitionMap extends EnumMap<TypeDefinitionCategory, Object> {

    public TypeDefinitionMap() {
        super(TypeDefinitionCategory.class);
    }

    public TypeDefinitionMap(TypeDefinitionMap copy) {
        this();
        for (Entry<TypeDefinitionCategory, Object> entry : copy.entrySet()) {
            this.setValue(entry.getKey(), entry.getValue());
        }
    }

    public TypeDefinitionMap setValue(TypeDefinitionCategory type, Object value) {
        super.put(type, value);
        return this;
    }

    public <O> O getValue(TypeDefinitionCategory type) {
        Object value = super.get(type);
        return (O) ((value == null) ? type.defaultValue(this) : value);
    }

    public <O> O getValue(TypeDefinitionCategory type, Class<O> clazz) {
        Object value = super.get(type);
        return (O) ((value == null) ? type.defaultValue(this) : value);
    }

    public void isValidDefinition(Set<TypeDefinitionCategory> requiredTypes) {
        Set<TypeDefinitionCategory> keys = this.keySet();
        for (TypeDefinitionCategory type : requiredTypes) {
            Preconditions.checkArgument(keys.contains(type), "%s not in %s", type, this);
        }
        Preconditions.checkArgument(keys.size() == requiredTypes.size(), "Found irrelevant definitions in: %s", this);
    }

    // note: special case takes into account that only one modifier is present on each type modifier vertex
    public void isValidTypeModifierDefinition(Set<TypeDefinitionCategory> legalTypes) {
        Preconditions.checkArgument(1 == this.size(), "exactly one type modifier is expected");
        for (TypeDefinitionCategory type : this.keySet()) {
            Preconditions.checkArgument(legalTypes.contains(type), "%s not legal here");
        }
    }
}
