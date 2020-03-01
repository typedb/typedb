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

package grakn.core.graph.graphdb.internal;

import com.google.common.primitives.Longs;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.graphdb.idmanagement.IDManager;
import grakn.core.graph.graphdb.relations.RelationIdentifier;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * AbstractElement is the base class for all elements in JanusGraph.
 * It is defined and uniquely identified by its id.
 * <p>
 * For the id, it holds that:
 * id&lt;0: Temporary id, will be assigned id&gt;0 when the transaction is committed
 * id=0: Virtual or implicit element that does not physically exist in the database
 * id&gt;0: Physically persisted element
 */
public abstract class AbstractElement implements InternalElement, Comparable<JanusGraphElement> {

    private long id;

    public AbstractElement(long id) {
        this.id = id;
    }

    private static boolean isTemporaryId(long elementId) {
        return elementId < 0;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(getCompareId());
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (this == other) {
            return true;
        }
        if (!((this instanceof Vertex && other instanceof Vertex) ||
                (this instanceof Edge && other instanceof Edge) ||
                (this instanceof VertexProperty && other instanceof VertexProperty))) {
            return false;
        }
        //Same type => they are the same if they have identical ids.
        if (other instanceof AbstractElement) {
            return getCompareId() == ((AbstractElement) other).getCompareId();
        } else if (other instanceof JanusGraphElement) {
            return ((JanusGraphElement) other).hasId() && getCompareId() == ((JanusGraphElement) other).longId();
        } else {
            Object otherId = ((Element) other).id();
            if (otherId instanceof RelationIdentifier) {
                return ((RelationIdentifier) otherId).getRelationId() == getCompareId();
            } else {
                return otherId.equals(getCompareId());
            }
        }
    }


    @Override
    public int compareTo(JanusGraphElement other) {
        return compare(this, other);
    }

    public static int compare(JanusGraphElement e1, JanusGraphElement e2) {
        long e1id = (e1 instanceof AbstractElement) ? ((AbstractElement) e1).getCompareId() : e1.longId();
        long e2id = (e2 instanceof AbstractElement) ? ((AbstractElement) e2).getCompareId() : e2.longId();
        return Longs.compare(e1id, e2id);
    }

    @Override
    public InternalVertex clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    /* ---------------------------------------------------------------
     * ID and LifeCycle methods
     * ---------------------------------------------------------------
     */

    /**
     * Long identifier used to compare elements. Often, this is the same as #longId()
     * but some instances of elements may be considered the same even if their ids differ. In that case,
     * this method should be overwritten to return an id that can be used for comparison.
     */
    protected long getCompareId() {
        return longId();
    }

    @Override
    public long longId() {
        return id;
    }

    public boolean hasId() {
        return !isTemporaryId(longId());
    }

    @Override
    public void setId(long id) {
        this.id = id;
    }

    @Override
    public boolean isInvisible() {
        return IDManager.VertexIDType.Invisible.is(id);
    }

    @Override
    public boolean isNew() {
        return ElementLifeCycle.isNew(it().getLifeCycle());
    }

    @Override
    public boolean isLoaded() {
        return ElementLifeCycle.isLoaded(it().getLifeCycle());
    }

    @Override
    public boolean isRemoved() {
        return ElementLifeCycle.isRemoved(it().getLifeCycle());
    }

}
