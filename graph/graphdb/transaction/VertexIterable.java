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

package grakn.core.graph.graphdb.transaction;

import grakn.core.graph.diskstorage.util.RecordIterator;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.graph.graphdb.idmanagement.IDManager;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;

import java.util.Iterator;
import java.util.NoSuchElementException;


public class VertexIterable implements Iterable<InternalVertex> {

    private final StandardJanusGraphTx tx;
    private final StandardJanusGraph graph;

    VertexIterable(StandardJanusGraph graph, StandardJanusGraphTx tx) {
        this.graph = graph;
        this.tx = tx;
    }

    @Override
    public Iterator<InternalVertex> iterator() {
        return new Iterator<InternalVertex>() {

            private final RecordIterator<Long> iterator = graph.getVertexIDs(tx.getBackendTransaction());
            private InternalVertex nextVertex = nextVertex();

            private InternalVertex nextVertex() {
                InternalVertex v = null;
                while (v == null && iterator.hasNext()) {
                    long nextId = iterator.next();
                    //Skip invisible vertices
                    if (!IDManager.VertexIDType.Invisible.is(nextId)) {
                        v = tx.getInternalVertex(nextId);
                        //Filter out deleted vertices and types
                        if (v.isRemoved()) v = null;
                    }
                }
                return v;
            }

            @Override
            public boolean hasNext() {
                return nextVertex != null;
            }

            @Override
            public InternalVertex next() {
                if (!hasNext()) throw new NoSuchElementException();
                InternalVertex returnVertex = nextVertex;
                nextVertex = nextVertex();
                return returnVertex;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
