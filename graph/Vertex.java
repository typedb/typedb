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

public abstract class Vertex {

    private final GraphManager graph;
    private final byte[] iid;

    private Schema.Status status;
    private Schema.Vertex clazz;

    private Vertex(GraphManager graph, Schema.Status status, Schema.Vertex type, byte[] iid) {
        this.graph = graph;
        this.iid = iid;

        this.status = status;
    }

    public Schema.Status status() {
        return status;
    }

    public byte[] iid() {
        return iid;
    }

    public static class Type extends Vertex {

        private final String label;

        private boolean isAbstract;
        private Schema.DataType dataType;
        private String regex;

        Type(GraphManager graph, Schema.Status status, Schema.Vertex.Type type, byte[] iid, String label) {
            super(graph, status, type, iid);
            this.label = label;
        }

        public String label() {
            return label;
        }

        public boolean isAbstract() {
            return isAbstract;
        }

        public Vertex.Type setAbstract(boolean isAbstract) {
            this.isAbstract = isAbstract;
            return this;
        }

        public Schema.DataType dataType() {
            return dataType;
        }

        public Vertex.Type dataType(Schema.DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public String regex() {
            return regex;
        }

        public Vertex.Type regex(String regex) {
            this.regex = regex;
            return this;
        }
    }

    public static class Thing extends Vertex {

        Thing(GraphManager graph, Schema.Status status, Schema.Vertex.Thing type, byte[] iid) {
            super(graph, status, type, iid);
        }
    }

    public static class Value extends Vertex {

        private final Object value;

        Value(GraphManager graph, Schema.Status status, byte[] iid, Object value) {
            super(graph, status, Schema.Vertex.Other.VALUE, iid);
            this.value = value;
        }

    }
}
