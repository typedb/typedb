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

package grakn.core.graph.graphdb.util;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.internal.RelationCategory;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class VertexCentricEdgeIterable<R extends JanusGraphRelation> implements Iterable<R> {

    private final Iterable<InternalVertex> vertices;
    private final RelationCategory relationCategory;

    public VertexCentricEdgeIterable(Iterable<InternalVertex> vertices, RelationCategory relationCategory) {
        Preconditions.checkArgument(vertices != null && relationCategory != null);
        this.vertices = vertices;
        this.relationCategory = relationCategory;
    }

    @Override
    public Iterator<R> iterator() {
        return new EdgeIterator();
    }

    private class EdgeIterator implements Iterator<R> {

        private final Iterator<InternalVertex> vertexIterator;
        private Iterator<JanusGraphRelation> currentOutEdges;
        private JanusGraphRelation nextEdge = null;

        public EdgeIterator() {
            this.vertexIterator = vertices.iterator();
            if (vertexIterator.hasNext()) {
                currentOutEdges = relationCategory.executeQuery(vertexIterator.next().query().direction(Direction.OUT)).iterator();
                getNextEdge();
            }
        }

        private void getNextEdge() {
            nextEdge = null;
            while (nextEdge == null) {
                if (currentOutEdges.hasNext()) {
                    nextEdge = currentOutEdges.next();
                    break;
                } else if (vertexIterator.hasNext()) {
                    currentOutEdges = relationCategory.executeQuery(vertexIterator.next().query().direction(Direction.OUT)).iterator();
                } else break;
            }
        }

        @Override
        public boolean hasNext() {
            return nextEdge != null;
        }

        @Override
        public R next() {
            if (nextEdge == null) throw new NoSuchElementException();
            JanusGraphRelation returnEdge = nextEdge;
            getNextEdge();
            return (R) returnEdge;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
