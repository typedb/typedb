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

package grakn.core.graph.graphdb.relations;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.graphdb.internal.InternalVertex;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;


public abstract class AbstractVertexProperty<V> extends AbstractTypedRelation implements JanusGraphVertexProperty<V> {

    private InternalVertex vertex;
    private final Object value;

    public AbstractVertexProperty(long id, PropertyKey type, InternalVertex vertex, Object value) {
        super(id, type);
        this.vertex = Preconditions.checkNotNull(vertex, "null vertex");
        this.value = Preconditions.checkNotNull(value, "null value for property key %s",type);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    public void setVertexAt(int pos, InternalVertex vertex) {
        Preconditions.checkArgument(pos==0 && vertex!=null && this.vertex.equals(vertex));
        this.vertex=vertex;
    }

    @Override
    public InternalVertex getVertex(int pos) {
        if (pos==0) return vertex;
        else throw new IllegalArgumentException("Invalid position: " + pos);
    }

    @Override
    public JanusGraphTransaction graph() {
        return vertex.graph();
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        return super.properties(propertyKeys);
    }

    @Override
    public final int getArity() {
        return 1;
    }

    @Override
    public final int getLen() {
        return 1;
    }

    @Override
    public JanusGraphVertex element() {
        return vertex;
    }

    @Override
    public V value() {
        return (V)value;
    }

    @Override
    public boolean isProperty() {
        return true;
    }

    @Override
    public boolean isEdge() {
        return false;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

}
