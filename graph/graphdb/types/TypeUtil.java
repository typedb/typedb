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
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.ConsistencyModifier;
import grakn.core.graph.graphdb.database.management.ModifierType;
import grakn.core.graph.graphdb.internal.ElementCategory;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

public class TypeUtil {

    public static boolean hasSimpleInternalVertexKeyIndex(JanusGraphRelation rel) {
        return rel instanceof JanusGraphVertexProperty && hasSimpleInternalVertexKeyIndex((JanusGraphVertexProperty) rel);
    }

    public static void checkTypeName(JanusGraphSchemaCategory category, String name) {
        switch (category) {
            case EDGELABEL:
            case VERTEXLABEL:
                if (name == null) throw Element.Exceptions.labelCanNotBeNull();
                if (StringUtils.isBlank(name)) throw Element.Exceptions.labelCanNotBeEmpty();
                break;
            case PROPERTYKEY:
                if (name == null) throw Property.Exceptions.propertyKeyCanNotBeNull();
                if (StringUtils.isBlank(name)) throw Property.Exceptions.propertyKeyCanNotBeEmpty();
                break;
            case GRAPHINDEX:
                Preconditions.checkArgument(StringUtils.isNotBlank(name), "Index name cannot be empty: %s", name);
                break;
            default:
                throw new AssertionError(category);
        }
    }

    private static boolean hasSimpleInternalVertexKeyIndex(JanusGraphVertexProperty prop) {
        return hasSimpleInternalVertexKeyIndex(prop.propertyKey());
    }

    public static boolean hasSimpleInternalVertexKeyIndex(PropertyKey key) {
        InternalRelationType type = (InternalRelationType) key;
        for (IndexType index : type.getKeyIndexes()) {
            if (index.getElement() == ElementCategory.VERTEX && index.isCompositeIndex()) {
                if (index.indexesKey(key)) return true;
            }
        }
        return false;
    }

    public static InternalRelationType getBaseType(InternalRelationType type) {
        InternalRelationType baseType = type.getBaseType();
        if (baseType == null) return type;
        else return baseType;
    }

    private static <T> T getTypeModifier(SchemaSource schema, ModifierType modifierType, T defaultValue) {
        for (SchemaSource.Entry entry : schema.getRelated(TypeDefinitionCategory.TYPE_MODIFIER, Direction.OUT)) {
            T value = entry.getSchemaType().getDefinition().getValue(modifierType.getCategory());
            if (null != value) {
                return value;
            }
        }
        return defaultValue;
    }


    public static ConsistencyModifier getConsistencyModifier(SchemaSource schema) {
        return getTypeModifier(schema, ModifierType.CONSISTENCY, ConsistencyModifier.DEFAULT);
    }

    public static int getTTL(SchemaSource schema) {
        return getTypeModifier(schema, ModifierType.TTL, 0);
    }
}
