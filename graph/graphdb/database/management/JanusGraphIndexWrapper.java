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

package grakn.core.graph.graphdb.database.management;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import grakn.core.graph.core.Cardinality;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.schema.JanusGraphIndex;
import grakn.core.graph.core.schema.Parameter;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.graphdb.types.CompositeIndexType;
import grakn.core.graph.graphdb.types.IndexField;
import grakn.core.graph.graphdb.types.IndexType;
import grakn.core.graph.graphdb.types.MixedIndexType;
import org.apache.tinkerpop.gremlin.structure.Element;


public class JanusGraphIndexWrapper implements JanusGraphIndex {

    private final IndexType index;

    public JanusGraphIndexWrapper(IndexType index) {
        this.index = index;
    }

    IndexType getBaseIndex() {
        return index;
    }

    @Override
    public String name() {
        return index.getName();
    }

    @Override
    public String getBackingIndex() {
        return index.getBackingIndexName();
    }

    @Override
    public Class<? extends Element> getIndexedElement() {
        return index.getElement().getElementType();
    }

    @Override
    public PropertyKey[] getFieldKeys() {
        IndexField[] fields = index.getFieldKeys();
        PropertyKey[] keys = new PropertyKey[fields.length];
        for (int i = 0; i < fields.length; i++) {
            keys[i]=fields[i].getFieldKey();
        }
        return keys;
    }

    @Override
    public Parameter[] getParametersFor(PropertyKey key) {
        if (index.isCompositeIndex()) return new Parameter[0];
        return ((MixedIndexType)index).getField(key).getParameters();
    }

    @Override
    public boolean isUnique() {
        return !index.isMixedIndex() && ((CompositeIndexType) index).getCardinality() == Cardinality.SINGLE;
    }

    @Override
    public SchemaStatus getIndexStatus(PropertyKey key) {
        Preconditions.checkArgument(Sets.newHashSet(getFieldKeys()).contains(key),"Provided key is not part of this index: %s",key);
        if (index.isCompositeIndex()) return ((CompositeIndexType)index).getStatus();
        else return ((MixedIndexType)index).getField(key).getStatus();
    }

    @Override
    public boolean isCompositeIndex() {
        return index.isCompositeIndex();
    }

    @Override
    public boolean isMixedIndex() {
        return index.isMixedIndex();
    }

    @Override
    public String toString() {
        return name();
    }

}

