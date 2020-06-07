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
import com.google.common.collect.ImmutableSet;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.schema.ConsistencyModifier;
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.graphdb.database.management.ModifierType;
import grakn.core.graph.graphdb.internal.ElementCategory;
import grakn.core.graph.graphdb.internal.Order;
import grakn.core.graph.graphdb.internal.RelationCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Set;

public enum TypeDefinitionCategory {
    //Don't change the order because it breaks backward compatibility.
    //Relation Types
    INVISIBLE(Boolean.class),
    SORT_KEY(long[].class),
    SORT_ORDER(Order.class),
    SIGNATURE(long[].class),
    MULTIPLICITY(Multiplicity.class),
    DATATYPE(Class.class),
    UNIDIRECTIONAL(Direction.class),

    //General admin
    STATUS(SchemaStatus.class),

    //Index Types
    ELEMENT_CATEGORY(ElementCategory.class),
    INDEX_CARDINALITY(Cardinality.class),
    INTERNAL_INDEX(Boolean.class),
    BACKING_INDEX(String.class),
    INDEXSTORE_NAME(String.class),

    //Consistency Types
    CONSISTENCY_LEVEL(ConsistencyModifier.class),

    // type modifiers
    TTL(Integer.class),

    //Vertex Label
    PARTITIONED(Boolean.class),
    STATIC(Boolean.class),

    //Schema Edges
    RELATIONTYPE_INDEX(),
    TYPE_MODIFIER(),
    INDEX_FIELD(RelationCategory.EDGE, Parameter[].class),
    INDEX_SCHEMA_CONSTRAINT(),
    PROPERTY_KEY_EDGE(),
    CONNECTION_EDGE(RelationCategory.EDGE, String.class),
    UPDATE_CONNECTION_EDGE();

    public static final Set<TypeDefinitionCategory> PROPERTYKEY_DEFINITION_CATEGORIES = ImmutableSet.of(STATUS, INVISIBLE, SORT_KEY, SORT_ORDER, SIGNATURE, MULTIPLICITY, DATATYPE);
    public static final Set<TypeDefinitionCategory> EDGELABEL_DEFINITION_CATEGORIES = ImmutableSet.of(STATUS, INVISIBLE, SORT_KEY, SORT_ORDER, SIGNATURE, MULTIPLICITY, UNIDIRECTIONAL);
    public static final Set<TypeDefinitionCategory> INDEX_DEFINITION_CATEGORIES = ImmutableSet.of(STATUS, ELEMENT_CATEGORY,INDEX_CARDINALITY,INTERNAL_INDEX, BACKING_INDEX,INDEXSTORE_NAME);
    public static final Set<TypeDefinitionCategory> VERTEXLABEL_DEFINITION_CATEGORIES = ImmutableSet.of(PARTITIONED, STATIC);
    public static final Set<TypeDefinitionCategory> TYPE_MODIFIER_DEFINITION_CATEGORIES;

    static {
        ImmutableSet.Builder<TypeDefinitionCategory> builder = ImmutableSet.builder();
        for (ModifierType type : ModifierType.values()) {
            builder.add(type.getCategory());
        }
        TYPE_MODIFIER_DEFINITION_CATEGORIES = builder.build();
    }

    private final RelationCategory relationCategory;
    private final Class dataType;

    TypeDefinitionCategory() {
        this(RelationCategory.EDGE,null);
    }

    TypeDefinitionCategory(Class<?> dataType) {
        this(RelationCategory.PROPERTY, dataType);
    }

    TypeDefinitionCategory(RelationCategory relCat, Class<?> dataType) {
        Preconditions.checkArgument(relCat!=null && relCat.isProper());
        Preconditions.checkArgument(relCat== RelationCategory.EDGE || dataType !=null);
        this.relationCategory = relCat;
        this.dataType = dataType;
    }

    public boolean hasDataType() {
        return dataType !=null;
    }

    public Class<?> getDataType() {
        Preconditions.checkState(hasDataType());
        return dataType;
    }

    public boolean isProperty() {
        return relationCategory== RelationCategory.PROPERTY;
    }

    public boolean isEdge() {
        return relationCategory== RelationCategory.EDGE;
    }

    public boolean verifyAttribute(Object attribute) {
        Preconditions.checkNotNull(dataType);
        return attribute != null && dataType.equals(attribute.getClass());
    }

    public Object defaultValue(TypeDefinitionMap map) {
        switch(this) {
            case SORT_ORDER: return Order.ASC;
            case STATUS: return SchemaStatus.ENABLED;
            default: return null;
        }
    }

}
