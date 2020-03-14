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

import hypergraph.graph.GraphManager;
import hypergraph.graph.Schema;

public abstract class TypeVertex extends Vertex {

    private final String label;

    TypeVertex(GraphManager graph, Schema.Status status, Schema.Vertex.Type type, byte[] iid, String label) {
        super(graph, status, type, iid);
        this.label = label;
    }

    public String label() {
        return label;
    }

    public abstract boolean isAbstract();

    public abstract TypeVertex setAbstract(boolean isAbstract);

    public abstract Schema.DataType dataType();

    public abstract TypeVertex dataType(Schema.DataType dataType);

    public abstract String regex();

    public abstract TypeVertex regex(String regex);

    public static class Buffered extends TypeVertex {

        private boolean isAbstract;
        private Schema.DataType dataType;
        private String regex;

        Buffered(GraphManager graph, Schema.Vertex.Type type, byte[] iid, String label) {
            super(graph, Schema.Status.BUFFERED, type, iid, label);
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
    }
}
