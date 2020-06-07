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

import com.google.common.collect.Iterables;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.graphdb.types.MixedIndexType;
import grakn.core.graph.graphdb.types.ParameterIndexField;
import grakn.core.graph.graphdb.types.SchemaSource;
import grakn.core.graph.graphdb.types.TypeDefinitionCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class MixedIndexTypeWrapper extends IndexTypeWrapper implements MixedIndexType {

    public MixedIndexTypeWrapper(SchemaSource base) {
        super(base);
    }

    @Override
    public boolean isCompositeIndex() {
        return false;
    }

    @Override
    public boolean isMixedIndex() {
        return true;
    }

    ParameterIndexField[] fields = null;

    @Override
    public ParameterIndexField[] getFieldKeys() {
        ParameterIndexField[] result = fields;
        if (result == null) {
            Iterable<SchemaSource.Entry> entries = base.getRelated(TypeDefinitionCategory.INDEX_FIELD, Direction.OUT);
            int numFields = Iterables.size(entries);
            result = new ParameterIndexField[numFields];
            int pos = 0;
            for (SchemaSource.Entry entry : entries) {

                result[pos++] = ParameterIndexField.of((PropertyKey) entry.getSchemaType(), (Parameter[]) entry.getModifier());
            }
            fields = result;
        }
        return result;
    }

    @Override
    public ParameterIndexField getField(PropertyKey key) {
        return (ParameterIndexField) super.getField(key);
    }

    @Override
    public void resetCache() {
        super.resetCache();
        fields = null;
    }

    @Override
    public String getStoreName() {
        return base.getDefinition().getValue(TypeDefinitionCategory.INDEXSTORE_NAME, String.class);
    }


}
