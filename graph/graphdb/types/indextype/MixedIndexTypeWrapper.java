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

package grakn.core.graph.graphdb.types.indextype;

import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.structure.Direction;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.graphdb.types.MixedIndexType;
import grakn.core.graph.graphdb.types.ParameterIndexField;
import grakn.core.graph.graphdb.types.SchemaSource;
import grakn.core.graph.graphdb.types.TypeDefinitionCategory;
import grakn.core.graph.graphdb.types.indextype.IndexTypeWrapper;

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
