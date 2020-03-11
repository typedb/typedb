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

package hypergraph.graph;

import hypergraph.storage.Storage;

import java.nio.ByteBuffer;

public abstract class Vertex {

    private final Storage storage;
    private final byte[] iid;

    private Vertex(Storage storage, byte[] iid) {
        this.storage = storage;
        this.iid = iid;
    }

    public byte[] iid() {
        return iid;
    }

    public static class Type extends Vertex {

        private final String label;
        private boolean isAbstract;

        Type(Storage storage, Schema.Vertex.Type type, String label) {
            super(storage, ByteBuffer.allocate(3)
                    .put(type.prefix().key())
                    .putShort(storage.keyGenerator().forType(type.root().label()))
                    .array());
            this.label = label;
        }

        public Vertex.Type setAbstract(boolean isAbstract) {
            this.isAbstract = isAbstract;
            return this;
        }

        public boolean isAbstract() {
            return isAbstract;
        }

        public String label() {
            return label;
        }
    }

    public static class Thing extends Vertex {

        Thing(Storage storage, Schema.Vertex.Thing thing, Vertex.Type type) {
            super(storage, ByteBuffer.allocate(11)
                    .put(thing.prefix().key())
                    .put(type.iid())
                    .putLong(storage.keyGenerator().forThing(type.label()))
                    .array());
        }
    }

//    public static class Value extends Vertex {
//
//        Value(Storage storage) {
//            super(storage);
//            Schema.Vertex type = Schema.Vertex.VALUE;
//        }
//
//    }
}
