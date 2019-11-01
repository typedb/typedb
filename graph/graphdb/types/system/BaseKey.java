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

package grakn.core.graph.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.Multiplicity;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.ConsistencyModifier;
import grakn.core.graph.core.schema.JanusGraphSchemaType;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.graphdb.internal.ElementCategory;
import grakn.core.graph.graphdb.internal.JanusGraphSchemaCategory;
import grakn.core.graph.graphdb.internal.Token;
import grakn.core.graph.graphdb.types.CompositeIndexType;
import grakn.core.graph.graphdb.types.IndexField;
import grakn.core.graph.graphdb.types.IndexType;
import grakn.core.graph.graphdb.types.TypeDefinitionDescription;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Collections;

public class BaseKey extends BaseRelationType implements PropertyKey {

    private enum Index {NONE, STANDARD, UNIQUE}

    //We rely on the vertex-existence property to be the smallest (in byte-order) when iterating over the entire graph
    public static final BaseKey VertexExists =
            new BaseKey("VertexExists", Boolean.class, 1, Index.NONE, Cardinality.SINGLE);

    public static final BaseKey SchemaName =
            new BaseKey("SchemaName", String.class, 32, Index.UNIQUE, Cardinality.SINGLE);

    public static final BaseKey SchemaDefinitionProperty =
            new BaseKey("SchemaDefinitionProperty", Object.class, 33, Index.NONE, Cardinality.LIST);

    public static final BaseKey SchemaCategory =
            new BaseKey("SchemaCategory", JanusGraphSchemaCategory.class, 34, Index.STANDARD, Cardinality.SINGLE);

    public static final BaseKey SchemaDefinitionDesc =
            new BaseKey("SchemaDefinitionDescription", TypeDefinitionDescription.class, 35, Index.NONE, Cardinality.SINGLE);

    public static final BaseKey SchemaUpdateTime =
            new BaseKey("SchemaUpdateTimestamp", Long.class, 36, Index.NONE, Cardinality.SINGLE);


    private final Class<?> dataType;
    private final Index index;
    private final Cardinality cardinality;

    private BaseKey(String name, Class<?> dataType, int id, Index index, Cardinality cardinality) {
        super(name, id, JanusGraphSchemaCategory.PROPERTYKEY);
        Preconditions.checkArgument(index != null && cardinality != null);
        this.dataType = dataType;
        this.index = index;
        this.cardinality = cardinality;
    }

    @Override
    public Class<?> dataType() {
        return dataType;
    }

    @Override
    public final boolean isPropertyKey() {
        return true;
    }

    @Override
    public final boolean isEdgeLabel() {
        return false;
    }

    @Override
    public Multiplicity multiplicity() {
        return Multiplicity.convert(cardinality());
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return dir == Direction.OUT;
    }

    @Override
    public Cardinality cardinality() {
        return cardinality;
    }

    @Override
    public Iterable<IndexType> getKeyIndexes() {
        if (index == Index.NONE) return Collections.EMPTY_LIST;
        return ImmutableList.of(indexDef);
    }

    private final CompositeIndexType indexDef = new CompositeIndexType() {

        private final IndexField[] fields = {IndexField.of(BaseKey.this)};

        @Override
        public String toString() {
            return getName();
        }

        @Override
        public long getID() {
            return BaseKey.this.longId();
        }

        @Override
        public IndexField[] getFieldKeys() {
            return fields;
        }

        @Override
        public IndexField getField(PropertyKey key) {
            if (key.equals(BaseKey.this)) return fields[0];
            else return null;
        }

        @Override
        public boolean indexesKey(PropertyKey key) {
            return getField(key) != null;
        }

        @Override
        public Cardinality getCardinality() {
            switch (index) {
                case UNIQUE:
                    return Cardinality.SINGLE;
                case STANDARD:
                    return Cardinality.LIST;
                default:
                    throw new AssertionError();
            }
        }

        @Override
        public ConsistencyModifier getConsistencyModifier() {
            return ConsistencyModifier.LOCK;
        }

        @Override
        public ElementCategory getElement() {
            return ElementCategory.VERTEX;
        }

        @Override
        public boolean hasSchemaTypeConstraint() {
            return false;
        }

        @Override
        public JanusGraphSchemaType getSchemaTypeConstraint() {
            return null;
        }

        @Override
        public boolean isCompositeIndex() {
            return true;
        }

        @Override
        public boolean isMixedIndex() {
            return false;
        }

        @Override
        public String getBackingIndexName() {
            return Token.INTERNAL_INDEX_NAME;
        }

        @Override
        public String getName() {
            return "SystemIndex#" + BaseKey.this.name();
        }

        @Override
        public SchemaStatus getStatus() {
            return SchemaStatus.ENABLED;
        }

        @Override
        public void resetCache() {
        }

        //Use default hashcode and equals
    };

}
