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

package graql.lang.util;

public class Token {

    /**
     * Graql commands to determine the type of query
     */
    public enum Command {
        COMPUTE("compute"),
        MATCH("match"),
        DEFINE("define"),
        UNDEFINE("undefine"),
        INSERT("insert"),
        DELETE("delete"),
        GET("get"),
        AGGREGATE("aggregate"),
        GROUP("group"),
        OFFSET("offset"),
        LIMIT("limit");

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

    public enum Filter {
        SORT("sort"),
        OFFSET("offset"),
        LIMIT("limit");

        private final String filter;

        Filter(String filter) {
            this.filter = filter;
        }

        @Override
        public String toString() {
            return this.filter;
        }

        public static Filter of(String value) {
            for (Filter c : Filter.values()) {
                if (c.filter.equals(value)) {
                    return c;
                }
            }
            return null;
        }
    }

    /**
     * Characters available for use in the Graql syntax
     */
    public enum Char {
        EQUAL("="),
        COLON(":"),
        SEMICOLON(";"),
        SPACE(" "),
        COMMA(","),
        COMMA_SPACE(", "),
        CURLY_OPEN("{"),
        CURLY_CLOSE("}"),
        PARAN_OPEN("("),
        PARAN_CLOSE(")"),
        SQUARE_OPEN("["),
        SQUARE_CLOSE("]"),
        QUOTE("\""),
        NEW_LINE("\n"),
        UNDERSCORE("_"),
        $_("$_"),
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

    public enum Operator {
        AND("and"),
        OR("or"),
        NOT("not");

        private final String operator;

        Operator(String operator) {
            this.operator = operator;
        }

        @Override
        public String toString() {
            return this.operator;
        }

        public static Operator of(String value) {
            for (Operator c : values()) {
                if (c.operator.equals(value)) {
                    return c;
                }
            }
            return null;
        }
    }

    public enum Comparator {
        EQ("="),
        NEQ("!="),
        EQV("=="),
        NEQV("!=="),
        GT(">"),
        GTE(">="),
        LT("<"),
        LTE("<="),
        CONTAINS("contains"), // TODO: remove duplicate in ComputeQuery.Param
        LIKE("like");

        private final String comparator;

        Comparator(String comparator) {
            this.comparator = comparator;
        }

        @Override
        public String toString() {
            return this.comparator;
        }

        public static Comparator of(String value) {
            for (Comparator c : values()) {
                if (c.comparator.equals(value)) {
                    return c;
                }
            }
            return null;
        }
    }

    public enum Property {
        DATA_TYPE("datatype"),
        HAS("has"),
        KEY("key"),
        VIA("via"),
        ID("id"),
        ABSTRACT("abstract"),
        ISA("isa"),
        ISAX("isa!"),
        TYPE("type"),
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

    public enum DataType {
        BOOLEAN("boolean"),
        DATE("date"),
        DOUBLE("double"),
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

    public enum Order {
        ASC("asc"),
        DESC("desc");

        private final String order;

        Order(String order) {
            this.order = order;
        }

        @Override
        public String toString() {
            return this.order;
        }

        public static Order of(String value) {
            for (Order c : values()) {
                if (c.order.equals(value)) {
                    return c;
                }
            }
            return null;
        }
    }

    public enum Literal {
        TRUE("true"),
        FALSE("false");

        private final String literal;

        Literal(String type) {
            this.literal = type;
        }

        @Override
        public String toString() {
            return this.literal;
        }

        public static Literal of(String value) {
            for (Literal c : values()) {
                if (c.literal.equals(value)) {
                    return c;
                }
            }
            return null;
        }
    }

    public static class Statistics {

        public enum Method {
            COUNT("count"),
            MAX("max"),
            MEAN("mean"),
            MEDIAN("median"),
            MIN("min"),
            STD("std"),
            SUM("sum");

            private final String method;

            Method(String method) {
                this.method = method;
            }

            @Override
            public String toString() {
                return this.method;
            }

            public static Method of(String value) {
                for (Method m : Method.values()) {
                    if (m.method.equals(value)) {
                        return m;
                    }
                }
                return null;
            }
        }
    }

    public static class Compute {

        /**
         * Graql Compute conditions keyword
         */
        public enum Condition {
            FROM("from"),
            TO("to"),
            OF("of"),
            IN("in"),
            USING("using"),
            WHERE("where");

            private final String condition;

            Condition(String algorithm) {
                this.condition = algorithm;
            }

            @Override
            public String toString() {
                return this.condition;
            }

            public static Condition of(String value) {
                for (Condition c : Condition.values()) {
                    if (c.condition.equals(value)) {
                        return c;
                    }
                }
                return null;
            }
        }

        /**
         * Graql Compute algorithm names
         */
        public enum Algorithm {
            DEGREE("degree"),
            K_CORE("k-core"),
            CONNECTED_COMPONENT("connected-component");

            private final String algorithm;

            Algorithm(String algorithm) {
                this.algorithm = algorithm;
            }

            @Override
            public String toString() {
                return this.algorithm;
            }

            public static Algorithm of(String value) {
                for (Algorithm a : Algorithm.values()) {
                    if (a.algorithm.equals(value)) {
                        return a;
                    }
                }
                return null;
            }
        }

        /**
         * Graql Compute parameter names
         */
        public enum Param {
            MIN_K("min-k"),
            K("k"),
            CONTAINS("contains"),
            SIZE("size");

            private final String param;

            Param(String param) {
                this.param = param;
            }

            @Override
            public String toString() {
                return this.param;
            }

            public static Param of(String value) {
                for (Param p : Param.values()) {
                    if (p.param.equals(value)) {
                        return p;
                    }
                }
                return null;
            }
        }
    }
}
