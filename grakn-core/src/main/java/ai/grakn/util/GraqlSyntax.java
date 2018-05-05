/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.util;

import ai.grakn.concept.ConceptId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static ai.grakn.util.GraqlSyntax.Char.EQUAL;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CENTRALITY;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CLUSTER;
import static ai.grakn.util.GraqlSyntax.Compute.Method.COUNT;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MAX;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MEAN;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MEDIAN;
import static ai.grakn.util.GraqlSyntax.Compute.Method.MIN;
import static ai.grakn.util.GraqlSyntax.Compute.Method.PATH;
import static ai.grakn.util.GraqlSyntax.Compute.Method.STD;
import static ai.grakn.util.GraqlSyntax.Compute.Method.SUM;

/**
 * Graql syntax keywords
 *
 * @author Haikal Pribadi
 */
public class GraqlSyntax {

    public static final String MATCH = "match";


    // Graql Queries
    public static final String COMPUTE = "compute";

    public enum Char {
        EQUAL("="),
        SEMICOLON(";"),
        SPACE(" "),
        COMMA(","),
        COMMA_SPACE(", "),
        SQUARE_OPEN("["),
        SQUARE_CLOSE("]"),
        QUOTE("\"");
        
        private final String character;

        Char(String character) {
            this.character = character;
        }

        @Override
        public String toString() {
            return this.character;
        }
    }
    /**
     * Graql Compute syntax keyword
     */
    public static class Compute {


        public enum Method {
            COUNT("setNumber"),
            MIN("min"),
            MAX("max"),
            MEDIAN("median"),
            MEAN("mean"),
            STD("std"),
            SUM("sum"),
            PATH("path"),
            CENTRALITY("centrality"),
            CLUSTER("cluster");

            private final String method;

            Method(String algorithm) {
                this.method = algorithm;
            }

            @Override
            public String toString() {
                return this.method;
            }
        }

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
        }

        /**
         * Graql Compute parameter names
         */
        public enum Parameter {
            MIN_K("min-k"),
            K("k"),
            CONTAINS("contains"),
            MEMBERS("members"),
            SIZE("size");

            private final String arg;

            Parameter(String arg) {
                this.arg = arg;
            }

            @Override
            public String toString() {
                return this.arg;
            }
        }

        //TODO: Move this class over into ComputeQuery (nested) once we replace Graql interfaces with classes
        public static class Argument<T> {

            public final static long DEFAULT_MIN_K = 2L;
            public final static long DEFAULT_K = 2L;
            public final static boolean DEFAULT_MEMBERS = false;

            public final static Map<Method, Boolean> DEFAULT_INCLUDE_ATTRIBUTES = defaultIncludeAttributes();

            private static Map<Method, Boolean> defaultIncludeAttributes() {
                Map<Method, Boolean> map = new HashMap<>();
                map.put(COUNT, false);
                map.put(MIN, true);
                map.put(MAX, true);
                map.put(MEDIAN, true);
                map.put(MEAN, true);
                map.put(STD, true);
                map.put(SUM, true);
                map.put(PATH, false);
                map.put(CENTRALITY, true);
                map.put(CLUSTER, false);

                return Collections.unmodifiableMap(map);
            }

            private Parameter param;
            private T arg;

            private Argument(Parameter param, T arg) {
                this.param = param;
                this.arg = arg;
            }

            public final Parameter type() {
                return this.param;
            }

            public final T get() {
                return this.arg;
            }

            public static Argument<Long> min_k(long minK) {
                return new Argument<>(Parameter.MIN_K, minK);
            }

            public static Argument<Long> k(long k) {
                return new Argument<>(Parameter.K, k);
            }

            public static Argument<Long> size(long size) {
                return new Argument<>(Parameter.SIZE, size);
            }

            public static Argument<Boolean> members(boolean members) {
                return new Argument<>(Parameter.MEMBERS, members);
            }

            public static Argument<ConceptId> contains(ConceptId conceptId) {
                return new Argument<>(Parameter.CONTAINS, conceptId);
            }

            @Override
            public String toString() {
                return param + EQUAL.toString() + arg.toString();
            }
        }
    }
}