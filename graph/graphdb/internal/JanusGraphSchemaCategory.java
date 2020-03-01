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

package grakn.core.graph.graphdb.internal;

import com.google.common.base.Preconditions;
import grakn.core.graph.graphdb.types.TypeDefinitionCategory;
import grakn.core.graph.graphdb.types.TypeDefinitionMap;
import grakn.core.graph.graphdb.types.TypeUtil;


public enum JanusGraphSchemaCategory {

    EDGELABEL, PROPERTYKEY, VERTEXLABEL, GRAPHINDEX, TYPE_MODIFIER;


    public boolean isRelationType() {
        return this== EDGELABEL || this== PROPERTYKEY;
    }

    public boolean hasName() {
        switch(this) {
            case EDGELABEL:
            case PROPERTYKEY:
            case GRAPHINDEX:
            case VERTEXLABEL:
                return true;
            case TYPE_MODIFIER:
                return false;
            default: throw new AssertionError();
        }
    }

    public String getSchemaName(String name) {
        Preconditions.checkState(hasName());
        TypeUtil.checkTypeName(this,name);
        String prefix;
        switch(this) {
            case EDGELABEL:
            case PROPERTYKEY:
                prefix = "rt";
                break;
            case GRAPHINDEX:
                prefix = "gi";
                break;
            case VERTEXLABEL:
                prefix = "vl";
                break;
            default: throw new AssertionError();
        }
        return Token.getSeparatedName(prefix,name);
    }

    public static String getRelationTypeName(String name) {
        return EDGELABEL.getSchemaName(name);
    }

    public static String getName(String schemaName) {
        String[] comps = Token.splitSeparatedName(schemaName);
        Preconditions.checkArgument(comps.length==2);
        return comps[1];
    }

    public void verifyValidDefinition(TypeDefinitionMap definition) {

        switch(this) {
            case EDGELABEL:
                definition.isValidDefinition(TypeDefinitionCategory.EDGELABEL_DEFINITION_CATEGORIES);
                break;
            case PROPERTYKEY:
                definition.isValidDefinition(TypeDefinitionCategory.PROPERTYKEY_DEFINITION_CATEGORIES);
                break;
            case GRAPHINDEX:
                definition.isValidDefinition(TypeDefinitionCategory.INDEX_DEFINITION_CATEGORIES);
                break;
            case TYPE_MODIFIER:
                definition.isValidTypeModifierDefinition(TypeDefinitionCategory.TYPE_MODIFIER_DEFINITION_CATEGORIES);
                break;
            case VERTEXLABEL:
                definition.isValidDefinition(TypeDefinitionCategory.VERTEXLABEL_DEFINITION_CATEGORIES);
                break;
            default: throw new AssertionError();
        }
    }


}
