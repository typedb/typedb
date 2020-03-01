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
import com.google.common.collect.Iterables;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.ConsistencyModifier;
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.graphdb.types.CompositeIndexType;
import grakn.core.graph.graphdb.types.IndexField;
import grakn.core.graph.graphdb.types.ParameterType;
import grakn.core.graph.graphdb.types.SchemaSource;
import grakn.core.graph.graphdb.types.TypeDefinitionCategory;
import grakn.core.graph.graphdb.types.TypeUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class CompositeIndexTypeWrapper extends IndexTypeWrapper implements CompositeIndexType {

    public CompositeIndexTypeWrapper(SchemaSource base) {
        super(base);
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
    public long getID() {
        return base.longId();
    }

    @Override
    public SchemaStatus getStatus() {
        return base.getStatus();
    }

    IndexField[] fields = null;

    @Override
    public IndexField[] getFieldKeys() {
        IndexField[] result = fields;
        if (result == null) {
            Iterable<SchemaSource.Entry> entries = base.getRelated(TypeDefinitionCategory.INDEX_FIELD, Direction.OUT);
            int numFields = Iterables.size(entries);
            result = new IndexField[numFields];
            for (SchemaSource.Entry entry : entries) {
                Integer value = ParameterType.INDEX_POSITION.findParameter((Parameter[]) entry.getModifier(), null);
                Preconditions.checkNotNull(value);
                int pos = value;
                Preconditions.checkArgument(pos >= 0 && pos < numFields, "Invalid field position: %s", pos);
                result[pos] = IndexField.of((PropertyKey) entry.getSchemaType());
            }
            fields = result;
        }
        return result;
    }

    @Override
    public void resetCache() {
        super.resetCache();
        fields = null;
    }

    @Override
    public Cardinality getCardinality() {
        return base.getDefinition().getValue(TypeDefinitionCategory.INDEX_CARDINALITY, Cardinality.class);
    }

    private ConsistencyModifier consistency = null;

    public ConsistencyModifier getConsistencyModifier() {
        if (consistency == null) {
            consistency = TypeUtil.getConsistencyModifier(base);
        }
        return consistency;
    }
}
