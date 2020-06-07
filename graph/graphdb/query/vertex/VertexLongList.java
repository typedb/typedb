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

package grakn.core.graph.graphdb.query.vertex;

import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.VertexList;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An implementation of VertexListInternal that stores only the vertex ids
 * and simply wraps an ArrayList and keeps a boolean flag to remember whether this list is in sort order.
 * In addition, we need a transaction reference in order to construct actual vertex references on request.
 * <p>
 * This is a more efficient way to represent a vertex result set but only applies to loaded vertices that have ids.
 * So, compared to VertexArrayList this is an optimization for the special use case that a vertex is loaded.
 */
public class VertexLongList implements VertexListInternal {

    private final StandardJanusGraphTx tx;
    private List<Long> vertices;
    private boolean sorted;

    public VertexLongList(StandardJanusGraphTx tx, List<Long> vertices, boolean sorted) {
        this.tx = tx;
        this.vertices = vertices;
        this.sorted = sorted;
    }

    @Override
    public void add(JanusGraphVertex n) {
        if (!vertices.isEmpty()) sorted = sorted && vertices.get(vertices.size() - 1) <= n.longId();
        vertices.add(n.longId());
    }

    @Override
    public long getID(int pos) {
        return vertices.get(pos);
    }

    @Override
    public List<Long> getIDs() {
        return vertices;
    }

    @Override
    public JanusGraphVertex get(int pos) {
        return tx.getInternalVertex(getID(pos));
    }

    @Override
    public boolean isSorted() {
        return sorted;
    }

    @Override
    public VertexList subList(int fromPosition, int length) {
        List<Long> subList = new ArrayList<>(length);
        subList.addAll(vertices.subList(fromPosition, fromPosition + length));
        return new VertexLongList(tx, subList, sorted);
    }

    @Override
    public int size() {
        return vertices.size();
    }

    //TODO this seems to be used only in tests (indirectly) - verify and delete if not necessary
    @Override
    public void addAll(VertexList vertexlist) {
        List<Long> otherVertexIds;
        if (vertexlist instanceof VertexLongList) {
            otherVertexIds = ((VertexLongList) vertexlist).vertices;
        } else if (vertexlist instanceof VertexArrayList) {
            VertexArrayList other = (VertexArrayList) vertexlist;
            otherVertexIds = new ArrayList<>(other.size());
            for (int i = 0; i < other.size(); i++) otherVertexIds.add(other.getID(i));
        } else {
            throw new IllegalArgumentException("Unsupported vertex-list: " + vertexlist.getClass());
        }
        vertices.addAll(otherVertexIds);
        if (sorted && vertexlist.isSorted()) {
            //Merge join
            vertices.sort(Comparator.comparingLong(v -> v));
        }
    }

    @Override
    public Iterator<JanusGraphVertex> iterator() {
        return new Iterator<JanusGraphVertex>() {

            private int pos = -1;

            @Override
            public boolean hasNext() {
                return (pos + 1) < size();
            }

            @Override
            public JanusGraphVertex next() {
                if (!hasNext()) throw new NoSuchElementException();
                pos++;
                return get(pos);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Vertices cannot be removed from neighborhood list");
            }

        };
    }

}
