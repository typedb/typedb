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

    protected final Storage storage;

    private Vertex(Storage storage) {
        this.storage = storage;
    }

    public static class Type extends Vertex {

        private final byte[] iid;

        Type(Storage storage, Schema.Vertex.Type type, String label) {
            super(storage);
            ByteBuffer newIID = ByteBuffer.allocate(3);
            newIID.put(type.prefix());

            this.iid = newIID.array();
        }
    }
    public static class Thing extends Vertex {

        Thing(Storage storage, Schema.Vertex.Thing thing) {
            super(storage);
        }
    }

    public static class Value extends Vertex {

        Value(Storage storage) {
            super(storage);
            Schema.Vertex type = Schema.Vertex.VALUE;

        }

    }
}
