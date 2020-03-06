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

package hypergraph.storage;

public class Schema {

    /**
     * The values in this class will be used as 'prefixes' within an IID in the
     * of every object database, and must not overlap with each other.
     *
     * The size of a prefix is 1 byte; i.e. min-value = 0 and max-value = 255.
     */
    private static class Prefix {
        private static final int INDEX_TYPE = 0;
        private static final int VERTEX_ENTITY_TYPE = 20;
        private static final int VERTEX_RELATION_TYPE = 30;
        private static final int VERTEX_ROLE_TYPE = 40;
        private static final int VERTEX_ATTRIBUTE_TYPE = 50;
        private static final int VERTEX_ENTITY = 60;
        private static final int VERTEX_RELATION = 70;
        private static final int VERTEX_ROLE = 80;
        private static final int VERTEX_ATTRIBUTE = 90;
        private static final int VERTEX_RULE = 100;

    }

    /**
     * The values in this class will be used as 'infixes' between two IIDs of
     * two objects in the database, and must not overlap with each other.
     *
     * The size of a prefix is 1 byte; i.e. min-value = 0 and max-value = 255.
     */
    private static class Infix {
        private static final int PROPERTY_ABSTRACT = 0;
        private static final int PROPERTY_DATATYPE = 1;
        private static final int PROPERTY_REGEX = 2;
        private static final int PROPERTY_VALUE = 3;
        private static final int PROPERTY_WHEN = 4;
        private static final int PROPERTY_THEN = 5;
        private static final int EDGE_SUB_OUT = 20;
        private static final int EDGE_SUB_IN = 25;
        private static final int EDGE_ISA_OUT = 30;
        private static final int EDGE_ISA_IN = 35;
        private static final int EDGE_KEY_OUT = 40;
        private static final int EDGE_KEY_IN = 45;
        private static final int EDGE_HAS_OUT = 50;
        private static final int EDGE_HAS_IN = 55;
        private static final int EDGE_PLAYS_OUT = 60;
        private static final int EDGE_PLAYS_IN = 65;
        private static final int EDGE_RELATES_OUT = 70;
        private static final int EDGE_RELATES_IN = 75;
        private static final int EDGE_OPT_OUT = 100;
        private static final int EDGE_OPT_IN = 105;

    }

    public enum IndexType {
        TYPE(Prefix.INDEX_TYPE);

        private final byte key;

        IndexType(int key) {
            this.key = (byte) key;
        }
    }

    public enum VertexType {
        ENTITY_TYPE(Prefix.VERTEX_ENTITY_TYPE),
        RELATION_TYPE(Prefix.VERTEX_RELATION_TYPE),
        ROLE_TYPE(Prefix.VERTEX_ROLE_TYPE),
        ATTRIBUTE_TYPE(Prefix.VERTEX_ATTRIBUTE_TYPE),
        ENTITY(Prefix.VERTEX_ENTITY),
        RELATION(Prefix.VERTEX_RELATION),
        ROLE(Prefix.VERTEX_ROLE),
        ATTRIBUTE(Prefix.VERTEX_ATTRIBUTE),
        RULE(Prefix.VERTEX_RULE);

        private final byte key;

        VertexType(int key) {
            this.key = (byte) key;
        }
    }

    public enum PropertyType {
        ABSTRACT(Infix.PROPERTY_ABSTRACT),
        DATATYPE(Infix.PROPERTY_DATATYPE),
        REGEX(Infix.PROPERTY_REGEX),
        VALUE(Infix.PROPERTY_VALUE),
        WHEN(Infix.PROPERTY_WHEN),
        THEN(Infix.PROPERTY_THEN);

        private final byte key;

        PropertyType(int key) {
            this.key = (byte) key;
        }
    }

    public enum DataType {
        LONG(0),
        DOUBLE(2),
        STRING(4),
        BOOLEAN(6),
        DATE(8);

        private final byte key;

        DataType(int key) {
            this.key = (byte) key;
        }
    }

    public enum EdgeType {
        SUB_OUT(Infix.EDGE_SUB_OUT),
        SUB_IN(Infix.EDGE_SUB_IN),
        ISA_OUT(Infix.EDGE_ISA_OUT),
        ISA_IN(Infix.EDGE_ISA_IN),
        KEY_OUT(Infix.EDGE_KEY_OUT),
        KEY_IN(Infix.EDGE_KEY_IN),
        HAS_OUT(Infix.EDGE_HAS_OUT),
        HAS_IN(Infix.EDGE_HAS_IN),
        PLAYS_OUT(Infix.EDGE_PLAYS_OUT),
        PLAYS_IN(Infix.EDGE_PLAYS_IN),
        RELATES_OUT(Infix.EDGE_RELATES_OUT),
        RELATES_IN(Infix.EDGE_RELATES_IN),
        OPT_ROLE_OUT(Infix.EDGE_OPT_OUT),
        OPT_ROLE_IN(Infix.EDGE_OPT_IN);

        private final byte key;

        EdgeType(int key) {
            this.key = (byte) key;
        }
    }
}
