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

package grakn.core.graph.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import grakn.core.graph.core.InvalidElementException;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.VertexLabel;
import grakn.core.graph.graphdb.internal.AbstractElement;
import grakn.core.graph.graphdb.internal.ElementLifeCycle;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.query.vertex.VertexCentricQueryBuilder;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.VertexLabelVertex;
import grakn.core.graph.graphdb.types.system.BaseLabel;
import grakn.core.graph.graphdb.types.system.BaseVertexLabel;
import grakn.core.graph.graphdb.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;

public abstract class AbstractVertex extends AbstractElement implements InternalVertex, Vertex {

    private final StandardJanusGraphTx tx;

    AbstractVertex(StandardJanusGraphTx tx, long id) {
        super(id);
        this.tx = tx;
    }

    @Override
    public final InternalVertex it() {
        return this;
        //The logic in Janus used to be the following:
//        if (tx.isOpen()) {
//            return this;
//        }
//        InternalVertex next = (InternalVertex) tx.getNextTx().getVertex(longId());
//        if (next == null) throw InvalidElementException.removedException(this);
//        else return next;

        // But we have deleted the tx.getNextTx() method to make everything look less magical.
    }

    @Override
    public final StandardJanusGraphTx tx() {
        return tx; //See comment above, original code: tx.isOpen() ? tx : tx.getNextTx();
    }

    public final boolean isTxOpen() {
        return tx.isOpen();
    }

    @Override
    public long getCompareId() {
        if (tx.isPartitionedVertex(this)) return tx.getIdManager().getCanonicalVertexId(longId());
        else return longId();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Object id() {
        return longId();
    }

    @Override
    public boolean isModified() {
        return ElementLifeCycle.isModified(it().getLifeCycle());
    }

    private void verifyAccess() {
        if (isRemoved()) {
            throw InvalidElementException.removedException(this);
        }
    }


    //Changing edges

    @Override
    public synchronized void remove() {
        verifyAccess();
        Iterator<JanusGraphRelation> iterator = it().query().noPartitionRestriction().relations().iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
        //Remove all system types on the vertex
        for (JanusGraphRelation r : it().query().noPartitionRestriction().system().relations()) {
            r.remove();
        }
    }

    // JanusGraphRelation Iteration/Access

    @Override
    public String label() {
        return vertexLabel().name();
    }

    protected Vertex getVertexLabelInternal() {
        return Iterables.getOnlyElement(tx().query(this).noPartitionRestriction().type(BaseLabel.VertexLabelEdge).direction(Direction.OUT).vertices(), null);
    }

    @Override
    public VertexLabel vertexLabel() {
        Vertex label = getVertexLabelInternal();
        if (label == null) return BaseVertexLabel.DEFAULT_VERTEXLABEL;
        else return (VertexLabelVertex) label;
    }

    @Override
    public VertexCentricQueryBuilder query() {
        verifyAccess();
        return tx().query(this);
    }

    @Override
    public <O> O valueOrNull(PropertyKey key) {
        return (O) property(key.name()).orElse(null);
    }


    // Convenience Methods for JanusGraphElement Creation

    public <V> JanusGraphVertexProperty<V> property(String key, V value, Object... keyValues) {
        JanusGraphVertexProperty<V> p = tx().addProperty(it(), tx().getOrCreatePropertyKey(key, value), value);
        ElementHelper.attachProperties(p, keyValues);
        return p;
    }

    @Override
    public <V> JanusGraphVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        //NOTE that cardinality is ignored as we are enforcing these cheks at Grakn level
        JanusGraphVertexProperty<V> p = tx().addProperty(it(), tx().getOrCreatePropertyKey(key, value), value);
        ElementHelper.attachProperties(p, keyValues);
        return p;
    }

    @Override
    public JanusGraphEdge addEdge(String label, Vertex vertex, Object... keyValues) {
        Preconditions.checkArgument(vertex instanceof JanusGraphVertex, "Invalid vertex provided: %s", vertex);
        JanusGraphEdge edge = tx().addEdge(it(), (JanusGraphVertex) vertex, tx().getOrCreateEdgeLabel(label));
        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    public Iterator<Edge> edges(Direction direction, String... labels) {
        return (Iterator) query().direction(direction).labels(labels).edges().iterator();
    }

    public <V> Iterator<VertexProperty<V>> properties(String... keys) {
        return (Iterator) query().direction(Direction.OUT).keys(keys).properties().iterator();
    }

    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return (Iterator) query().direction(direction).labels(edgeLabels).vertices().iterator();

    }

}
