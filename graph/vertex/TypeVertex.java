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

package hypergraph.graph.vertex;

import hypergraph.graph.KeyGenerator;
import hypergraph.graph.Schema;
import hypergraph.graph.Storage;

import java.nio.ByteBuffer;

public abstract class TypeVertex extends Vertex {

    private static final int IID_SIZE = 3;
    protected final String label;
    protected Boolean isAbstract;
    protected Schema.DataType dataType;
    protected String regex;

    TypeVertex(Storage storage, Schema.Status status, Schema.Vertex.Type type, byte[] iid, String label) {
        super(storage, status, type, iid);
        this.label = label;
    }

    @Override
    public Schema.Vertex.Type schema() {
        return (Schema.Vertex.Type) super.schema();
    }

    public String label() {
        return label;
    }

    public static byte[] generateIID(KeyGenerator keyGenerator, Schema.Vertex.Type schema) {
        return ByteBuffer.allocate(IID_SIZE)
                .put(schema.prefix().key())
                .putShort(keyGenerator.forType(schema.root()))
                .array();
    }

    public static byte[] generateIndex(String label) {
        byte[] labelBytes = label.getBytes();
        return ByteBuffer.allocate(labelBytes.length + 1)
                .put(Schema.Index.TYPE.prefix().key())
                .put(labelBytes)
                .array();
    }

    public abstract boolean isAbstract();

    public abstract TypeVertex setAbstract(boolean isAbstract);

    public abstract Schema.DataType dataType();

    public abstract TypeVertex dataType(Schema.DataType dataType);

    public abstract String regex();

    public abstract TypeVertex regex(String regex);

    public static class Buffered extends TypeVertex {

        public Buffered(Storage storage, Schema.Vertex.Type schema, byte[] iid, String label) {
            super(storage, Schema.Status.BUFFERED, schema, iid, label);
        }

        public boolean isAbstract() {
            return isAbstract;
        }

        public TypeVertex setAbstract(boolean isAbstract) {
            this.isAbstract = isAbstract;
            return this;
        }

        public Schema.DataType dataType() {
            return dataType;
        }

        public TypeVertex dataType(Schema.DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public String regex() {
            return regex;
        }

        public TypeVertex regex(String regex) {
            this.regex = regex;
            return this;
        }

        @Override
        public void persist() {
            storage.put(iid);
            persistIndex();
            persistProperties();
            persistEdges();
        }

        void persistIndex() {
            byte[] labelBytes = label.getBytes();
            byte[] index = ByteBuffer.allocate(labelBytes.length + 1)
                    .put(Schema.Index.TYPE.prefix().key())
                    .put(labelBytes)
                    .array();
            storage.put(index, iid);
        }

        void persistProperties() {
            persistPropertyLabel();
            if (isAbstract != null && !isAbstract) persistPropertyAbstract();
            if (dataType != null) persistPropertyDataType();
            if (regex != null && !regex.isEmpty()) persistPropertyRegex();
        }

        void persistPropertyAbstract() {
            byte[] key = ByteBuffer.allocate(iid.length + 1)
                    .put(iid)
                    .put(Schema.Property.ABSTRACT.infix().key())
                    .array();
            storage.put(key);
        }

        void persistPropertyLabel() {
            byte[] key = ByteBuffer.allocate(iid.length + 1)
                    .put(iid)
                    .put(Schema.Property.LABEL.infix().key())
                    .array();
            storage.put(key, label.getBytes());
        }

        void persistPropertyDataType() {
            byte[] key = ByteBuffer.allocate(iid.length + 1)
                    .put(iid)
                    .put(Schema.Property.DATATYPE.infix().key())
                    .array();
            storage.put(key, new byte[]{dataType.value()});
        }

        void persistPropertyRegex() {
            byte[] key = ByteBuffer.allocate(iid.length + 1)
                    .put(iid)
                    .put(Schema.Property.REGEX.infix().key())
                    .array();
            storage.put(key, regex.getBytes());
        }

        void persistEdges() {
            final int prefixSize = this.iid().length + 1;
            outs.forEach((key, value) -> value.forEach(edge -> {
                storage.put(ByteBuffer.allocate(prefixSize + edge.to().iid().length)
                                    .put(iid)
                                    .put(key.out().key())
                                    .put(edge.from().iid())
                                    .array());
            }));
            ins.forEach((key, value) -> value.forEach(edge -> {
                storage.put(ByteBuffer.allocate(prefixSize + edge.from().iid().length)
                                    .put(iid)
                                    .put(key.in().key())
                                    .put(edge.from().iid())
                                    .array());
            }));
        }
    }

    public static class Persisted extends TypeVertex {


        public Persisted(Storage storage, byte[] iid, String label) {
            super(storage, Schema.Status.PERSISTED, Schema.Vertex.Type.of(iid[0]), iid, label);
        }

        @Override
        public boolean isAbstract() {
            if (isAbstract != null) return isAbstract;
            byte[] abs = storage.get(ByteBuffer.allocate(iid.length + 1)
                                             .put(iid)
                                             .put(Schema.Property.ABSTRACT.infix().key())
                                             .array());
            isAbstract = abs != null;
            return isAbstract;
        }

        @Override
        public TypeVertex setAbstract(boolean isAbstract) {
            return null;
        }

        @Override
        public Schema.DataType dataType() {
            if (dataType != null) return dataType;
            byte[] val = storage.get(ByteBuffer.allocate(iid.length + 1)
                                             .put(iid)
                                             .put(Schema.Property.DATATYPE.infix().key())
                                             .array());
            if (val != null) dataType = Schema.DataType.of(val[0]);
            return dataType;
        }

        @Override
        public TypeVertex dataType(Schema.DataType dataType) {
            return null;
        }

        @Override
        public String regex() {
            if (regex != null) return regex;
            byte[] val = storage.get(ByteBuffer.allocate(iid.length + 1)
                                             .put(iid)
                                             .put(Schema.Property.REGEX.infix().key())
                                             .array());
            if (val != null) regex = new String(val);
            return regex;
        }

        @Override
        public TypeVertex regex(String regex) {
            return null;
        }

        @Override
        public void persist() {

        }
    }
}
