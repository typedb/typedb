/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.query;

/**
 * A Graql query of any kind. May read and write to the graph.
 */
public interface Query {

    /**
     * Graql commands to determine the type of query
     */
    enum Command {
        MATCH("match"),
        DEFINE("define"),
        UNDEFINE("undefine"),
        INSERT("insert"),
        DELETE("delete"),
        GET("get"),
        AGGREGATE("aggregate"),
        GROUP("group"),
        COMPUTE("compute");

        private final String command;

        Command(String command) {
            this.command = command;
        }

        @Override
        public String toString() {
            return this.command;
        }

        public static Command of(String value) {
            for (Command c : Command.values()) {
                if (c.command.equals(value)) {
                    return c;
                }
            }
            return null;
        }
    }

    /**
     * Characters available for use in the Graql syntax
     */
    enum Char {
        EQUAL("="),
        SEMICOLON(";"),
        SPACE(" "),
        COMMA(","),
        COMMA_SPACE(", "),
        CURLY_OPEN("{"),
        CURLY_CLOSE("}"),
        SQUARE_OPEN("["),
        SQUARE_CLOSE("]"),
        QUOTE("\""),
        NEW_LINE("\n"),
        UNDERSCORE("_"),
        $("$");

        private final String character;

        Char(String character) {
            this.character = character;
        }

        @Override
        public String toString() {
            return this.character;
        }
    }

    enum Operator {
        EQUAL("="),
        OR("or");

        private final String operator;

        Operator(String operator) {
            this.operator = operator;
        }

        @Override
        public String toString() {
            return this.operator;
        }
    }

    enum Property {
        DATA_TYPE("datatype"),
        HAS("has"),
        KEY("key"),
        ID("id"),
        ABSTRACT("abstract"),
        ISA("isa"),
        ISAX("isa!"),
        LABEL("label"),
        NEQ("!="),
        PLAYS("plays"),
        REGEX("regex"),
        RELATES("relates"),
        RELATION("relationship"), // TODO: Relationship syntax need to be updated
        SUB("sub"),
        SUBX("sub!"),
        THEN("then"),
        WHEN("when"),
        VALUE("");

        private final String name;

        Property(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }

        public static Property of(String value) {
            for (Property c : values()) {
                if (c.name.equals(value)) {
                    return c;
                }
            }
            return null;
        }
    }

    enum DataType {
        BOOLEAN("boolean"),
        DATE("date"),
        DOUBLE("double"),
//        FLOAT("float"),
//        INTEGER("integer"),
        LONG("long"),
        STRING("string");

        private final String type;

        DataType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return this.type;
        }

        public static DataType of(String value) {
            for (DataType c : values()) {
                if (c.type.equals(value)) {
                    return c;
                }
            }
            return null;
        }
    }
}
