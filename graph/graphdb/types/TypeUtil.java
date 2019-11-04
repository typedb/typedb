// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    private static <T> T getTypeModifier(SchemaSource schema,
                                         final ModifierType modifierType,
                                         final T defaultValue) {
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
