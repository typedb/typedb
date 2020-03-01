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

package grakn.core.graph.graphdb.types.indextype;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.JanusGraphSchemaType;
import grakn.core.graph.graphdb.internal.ElementCategory;
import grakn.core.graph.graphdb.types.IndexField;
import grakn.core.graph.graphdb.types.IndexType;
import grakn.core.graph.graphdb.types.SchemaSource;
import grakn.core.graph.graphdb.types.TypeDefinitionCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Map;


public abstract class IndexTypeWrapper implements IndexType {

    protected final SchemaSource base;

    IndexTypeWrapper(SchemaSource base) {
        Preconditions.checkNotNull(base);
        this.base = base;
    }

    public SchemaSource getSchemaBase() {
        return base;
    }

    @Override
    public ElementCategory getElement() {
        return base.getDefinition().getValue(TypeDefinitionCategory.ELEMENT_CATEGORY, ElementCategory.class);
    }

    @Override
    public int hashCode() {
        return base.hashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) {
            return true;
        } else if (!getClass().isInstance(oth)) {
            return false;
        }
        IndexTypeWrapper other = (IndexTypeWrapper) oth;
        return base.equals(other.base);
    }

    @Override
    public String toString() {
        return base.name();
    }

    @Override
    public String getName() {
        return base.name();
    }

    private volatile Map<PropertyKey, IndexField> fieldMap = null;

    @Override
    public IndexField getField(PropertyKey key) {
        Map<PropertyKey, IndexField> result = fieldMap;
        if (result == null) {
            ImmutableMap.Builder<PropertyKey, IndexField> b = ImmutableMap.builder();
            for (IndexField f : getFieldKeys()) {
                b.put(f.getFieldKey(), f);
            }
            result = b.build();
            fieldMap = result;
        }
        return result.get(key);
    }

    private volatile boolean cachedTypeConstraint = false;
    private volatile JanusGraphSchemaType schemaTypeConstraint = null;

    @Override
    public boolean hasSchemaTypeConstraint() {
        return getSchemaTypeConstraint() != null;
    }

    @Override
    public JanusGraphSchemaType getSchemaTypeConstraint() {
        JanusGraphSchemaType constraint;
        if (!cachedTypeConstraint) {
            Iterable<SchemaSource.Entry> related = base.getRelated(TypeDefinitionCategory.INDEX_SCHEMA_CONSTRAINT, Direction.OUT);
            if (Iterables.isEmpty(related)) {
                constraint = null;
            } else {
                constraint = (JanusGraphSchemaType) Iterables.getOnlyElement(related, null).getSchemaType();
            }
            schemaTypeConstraint = constraint;
            cachedTypeConstraint = true;
        } else {
            constraint = schemaTypeConstraint;
        }
        return constraint;
    }

    @Override
    public void resetCache() {
        base.resetCache();
        fieldMap = null;
    }

    @Override
    public boolean indexesKey(PropertyKey key) {
        return getField(key) != null;
    }

    @Override
    public String getBackingIndexName() {
        return base.getDefinition().getValue(TypeDefinitionCategory.BACKING_INDEX, String.class);
    }

}
