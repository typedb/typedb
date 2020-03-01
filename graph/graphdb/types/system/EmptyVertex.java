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

package grakn.core.graph.graphdb.types.system;

import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.VertexLabel;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.graphdb.internal.ElementLifeCycle;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.query.vertex.VertexCentricQueryBuilder;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.util.datastructures.Retriever;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

public class EmptyVertex implements InternalVertex {

    private static final String errorName = "Empty vertex";

    // JanusGraphRelation Iteration/Access

    @Override
    public VertexCentricQueryBuilder query() {
        throw new UnsupportedOperationException(errorName + " do not support querying");
    }

    @Override
    public List<InternalRelation> getAddedRelations(Predicate<InternalRelation> query) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public EntryList loadRelations(SliceQuery query, Retriever<SliceQuery, EntryList> lookup) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public boolean hasLoadedRelations(SliceQuery query) {
        return false;
    }

    @Override
    public boolean hasRemovedRelations() {
        return false;
    }

    @Override
    public boolean hasAddedRelations() {
        return false;
    }

    @Override
    public String label() {
        return vertexLabel().name();
    }

    @Override
    public VertexLabel vertexLabel() {
        return BaseVertexLabel.DEFAULT_VERTEXLABEL;
    }

    @Override
    public <O> O valueOrNull(PropertyKey key) {
        if (key instanceof ImplicitKey) return ((ImplicitKey)key).computeProperty(this);
        return null;
    }

    @Override
    public <O> O value(String key) {
        if (!tx().containsPropertyKey(key)) throw Property.Exceptions.propertyDoesNotExist(this,key);
        O val = valueOrNull(tx().getPropertyKey(key));
        if (val==null) throw Property.Exceptions.propertyDoesNotExist(this,key);
        return val;
    }

    // Convenience Methods for JanusGraphElement Creation

    @Override
    public <V> JanusGraphVertexProperty<V> property(String key, V value, Object...
            keyValues) {
        throw new UnsupportedOperationException(errorName + " do not support incident properties");
    }

    @Override
    public <V> JanusGraphVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object...
            keyValues) {
        throw new UnsupportedOperationException(errorName + " do not support incident properties");
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return Collections.emptyIterator();
    }

    @Override
    public boolean addRelation(InternalRelation e) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public void removeRelation(InternalRelation e) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }

    @Override
    public JanusGraphEdge addEdge(String s, Vertex vertex, Object... keyValues) {
        throw new UnsupportedOperationException(errorName + " do not support incident edges");
    }


    // In Memory JanusGraphElement

    @Override
    public long longId() {
        throw new UnsupportedOperationException(errorName + " don't have an ID");
    }

    @Override
    public Object id() {
        return hasId() ? longId() : null;
    }

    @Override
    public boolean hasId() {
        return false;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(errorName + " cannot be removed");
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        return Collections.emptyIterator();
    }

    @Override
    public void setId(long id) {
        throw new UnsupportedOperationException(errorName + " don't have an id");
    }

    @Override
    public byte getLifeCycle() {
        return ElementLifeCycle.Loaded;
    }

    @Override
    public boolean isInvisible() {
        return false;
    }

    @Override
    public boolean isRemoved() {
        return false;
    }

    @Override
    public boolean isLoaded() {
        return true;
    }

    @Override
    public boolean isModified() {
        return false;
    }

    @Override
    public boolean isNew() {
        return false;
    }

    @Override
    public InternalVertex it() {
        return this;
    }

    @Override
    public StandardJanusGraphTx tx() {
        throw new UnsupportedOperationException(errorName + " don't have an associated transaction");
    }

}
