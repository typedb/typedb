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
 *
 */

package hypergraph.graph.edge;

import hypergraph.graph.Schema;
import hypergraph.graph.Storage;
import hypergraph.graph.vertex.TypeVertex;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class TypeEdge extends Edge<Schema.Edge.Type, TypeVertex> {

    TypeEdge(Schema.Edge.Type schema, TypeVertex from, TypeVertex to) {
        super(schema, from, to);
    }

    public static class Buffered extends TypeEdge {

        private Storage storage;
        private AtomicBoolean committed;

        public Buffered(Storage storage, Schema.Edge.Type schema, TypeVertex from, TypeVertex to) {
            super(schema, from, to);
            this.storage = storage;
            committed = new AtomicBoolean(false);
        }

        public Schema.Status status() {
            return committed.get() ? Schema.Status.COMMITTED : Schema.Status.BUFFERED;
        }

        public void commit() {
            if (committed.compareAndSet(false, true)) {
                if (schema.out() != null) {
                    storage.put(ByteBuffer.allocate(from.iid().length + to().iid().length + 1)
                                        .put(from.iid()).put(schema.out().key()).put(to.iid()).array());
                }
                if (schema.in() != null) {
                    storage.put(ByteBuffer.allocate(from.iid().length + to().iid().length + 1)
                                        .put(to.iid()).put(schema.in().key()).put(from.iid()).array());
                }
            }
        }
    }

    public static class Persisted extends TypeEdge {

        public Persisted(Schema.Edge.Type schema, TypeVertex from, TypeVertex to) {
            super(schema, from, to);
        }

        public Schema.Status status() {
            return Schema.Status.PERSISTED;
        }

        public void commit() {}
    }
}
