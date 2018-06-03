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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.CONNECTED_COMPONENT;
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.DEGREE;
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.K_CORE;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.FROM;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.IN;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.OF;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.TO;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.USING;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.WHERE;
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
import static ai.grakn.util.GraqlSyntax.Compute.Parameter.CONTAINS;
import static ai.grakn.util.GraqlSyntax.Compute.Parameter.K;
import static ai.grakn.util.GraqlSyntax.Compute.Parameter.MEMBERS;
import static ai.grakn.util.GraqlSyntax.Compute.Parameter.MIN_K;
import static ai.grakn.util.GraqlSyntax.Compute.Parameter.SIZE;

/**
 * Graql syntax keywords
 * TODO: the content of this class (the enums) should be moved to inside Graql class, once we move it to this package.
 *
 * @author Grakn Warriors
 */
public class GraqlSyntax {

    /**
     * Graql commands to determine the type of query
     */
    public enum Command {
        MATCH("match"),
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
            for (Command c : Command.values())
                if (c.command.equals(value)) {
                    return c;
                }
            return null;
        }
    }

    /**
     * Characters available for use in the Graql syntax
     */
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
     * Graql Compute keywords to determine the compute method and conditions
     */
    public static class Compute {

        public final static Collection<Method> METHODS_ACCEPTED = ImmutableList.copyOf(Arrays.asList(Method.values()));

        public final static Map<Method, Collection<Condition>> CONDITIONS_REQUIRED = conditionsRequired();
        public final static Map<Method, Collection<Condition>> CONDITIONS_OPTIONAL = conditionsOptional();
        public final static Map<Method, Collection<Condition>> CONDITIONS_ACCEPTED = conditionsAccepted();

        public final static Map<Method, Algorithm> ALGORITHMS_DEFAULT = algorithmsDefault();
        public final static Map<Method, Collection<Algorithm>> ALGORITHMS_ACCEPTED = algorithmsAccepted();

        public final static Map<Method, Map<Algorithm, Collection<Parameter>>> ARGUMENTS_ACCEPTED = argumentsAccepted();
        public final static Map<Method, Map<Algorithm, Map<Parameter, Object>>> ARGUMENTS_DEFAULT = argumentsDefault();

        public final static Map<Method, Boolean> INCLUDE_ATTRIBUTES_DEFAULT = includeAttributesDefault();

        private static Map<Method, Collection<Condition>> conditionsRequired() {
            Map<Method, Collection<Condition>> required = new HashMap<>();
            required.put(MIN, ImmutableSet.of(OF));
            required.put(MAX, ImmutableSet.of(OF));
            required.put(MEDIAN, ImmutableSet.of(OF));
            required.put(MEAN, ImmutableSet.of(OF));
            required.put(STD, ImmutableSet.of(OF));
            required.put(SUM, ImmutableSet.of(OF));
            required.put(PATH, ImmutableSet.of(FROM, TO));
            required.put(CENTRALITY, ImmutableSet.of(USING));
            ;
            required.put(CLUSTER, ImmutableSet.of(USING));

            return ImmutableMap.copyOf(required);
        }

        private static Map<Method, Collection<Condition>> conditionsOptional() {
            Map<Method, Collection<Condition>> optional = new HashMap<>();
            optional.put(COUNT, ImmutableSet.of(IN));
            optional.put(MIN, ImmutableSet.of(IN));
            optional.put(MAX, ImmutableSet.of(IN));
            optional.put(MEDIAN, ImmutableSet.of(IN));
            optional.put(MEAN, ImmutableSet.of(IN));
            optional.put(STD, ImmutableSet.of(IN));
            optional.put(SUM, ImmutableSet.of(IN));
            optional.put(PATH, ImmutableSet.of(IN));
            optional.put(CENTRALITY, ImmutableSet.of(OF, IN, WHERE));
            optional.put(CLUSTER, ImmutableSet.of(IN, WHERE));

            return ImmutableMap.copyOf(optional);
        }

        private static Map<Method, Collection<Condition>> conditionsAccepted() {
            Map<Method, Collection<Condition>> accepted = new HashMap<>();

            for (Map.Entry<Method, Collection<Condition>> entry : CONDITIONS_REQUIRED.entrySet()) {
                accepted.put(entry.getKey(), new HashSet<>(entry.getValue()));
            }
            for (Map.Entry<Method, Collection<Condition>> entry : CONDITIONS_OPTIONAL.entrySet()) {
                if (accepted.containsKey(entry.getKey())) accepted.get(entry.getKey()).addAll(entry.getValue());
                else accepted.put(entry.getKey(), entry.getValue());
            }

            return ImmutableMap.copyOf(accepted);
        }

        private static Map<Method, Collection<Algorithm>> algorithmsAccepted() {
            Map<Method, Collection<Algorithm>> accepted = new HashMap<>();

            accepted.put(CENTRALITY, ImmutableSet.of(DEGREE, K_CORE));
            accepted.put(CLUSTER, ImmutableSet.of(CONNECTED_COMPONENT, K_CORE));

            return ImmutableMap.copyOf(accepted);
        }

        private static Map<Method, Map<Algorithm, Collection<Parameter>>> argumentsAccepted() {
            Map<Method, Map<Algorithm, Collection<Parameter>>> accepted = new HashMap<>();

            accepted.put(CENTRALITY, ImmutableMap.of(K_CORE, ImmutableSet.of(MIN_K)));
            accepted.put(CLUSTER, ImmutableMap.of(
                    K_CORE, ImmutableSet.of(K),
                    CONNECTED_COMPONENT, ImmutableSet.of(SIZE, MEMBERS, CONTAINS)
            ));

            return ImmutableMap.copyOf(accepted);
        }

        private static Map<Method, Map<Algorithm, Map<Parameter, Object>>> argumentsDefault() {
            Map<Method, Map<Algorithm, Map<Parameter, Object>>> defaults = new HashMap<>();

            defaults.put(CENTRALITY, ImmutableMap.of(K_CORE, ImmutableMap.of(MIN_K, Argument.DEFAULT_MIN_K)));
            defaults.put(CLUSTER, ImmutableMap.of(
                    K_CORE, ImmutableMap.of(K, Argument.DEFAULT_K),
                    CONNECTED_COMPONENT, ImmutableMap.of(MEMBERS, Argument.DEFAULT_MEMBERS)
            ));

            return defaults;
        }

        private static Map<Method, Algorithm> algorithmsDefault() {
            Map<Method, Algorithm> methodAlgorithm = new HashMap<>();
            methodAlgorithm.put(CENTRALITY, DEGREE);
            methodAlgorithm.put(CLUSTER, CONNECTED_COMPONENT);

            return methodAlgorithm;
        }

        private static Map<Method, Boolean> includeAttributesDefault() {
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

        /**
         * Graql compute method types to determine the type of calculation to execute
         */
        public enum Method {
            COUNT("count"),
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

            Method(String method) {
                this.method = method;
            }

            @Override
            public String toString() {
                return this.method;
            }

            public static Method of(String value) {
                for (Method m : Method.values())
                    if (m.method.equals(value)) {
                        return m;
                    }
                return null;
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

            public static Condition of(String value) {
                for (Condition c : Condition.values())
                    if (c.condition.equals(value)) {
                        return c;
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
                for (Algorithm a : Algorithm.values())
                    if (a.algorithm.equals(value)) {
                        return a;
                    }
                return null;
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

            private final String param;

            Parameter(String param) {
                this.param = param;
            }

            @Override
            public String toString() {
                return this.param;
            }

            public static Parameter of(String value) {
                for (Parameter p : Parameter.values())
                    if (p.param.equals(value)) {
                        return p;
                    }
                return null;
            }
        }

        /**
         * Graql Compute argument objects to be passed into the query
         * TODO: Move this class over into ComputeQuery (nested) once we replace Graql interfaces with classes
         *
         * @param <T>
         */
        public static class Argument<T> {

            public final static long DEFAULT_MIN_K = 2L;
            public final static long DEFAULT_K = 2L;
            public final static boolean DEFAULT_MEMBERS = false;

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
                return new Argument<>(MIN_K, minK);
            }

            public static Argument<Long> k(long k) {
                return new Argument<>(K, k);
            }

            public static Argument<Long> size(long size) {
                return new Argument<>(SIZE, size);
            }

            public static Argument<Boolean> members(boolean members) {
                return new Argument<>(MEMBERS, members);
            }

            public static Argument<ConceptId> contains(ConceptId conceptId) {
                return new Argument<>(CONTAINS, conceptId);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Argument that = (Argument) o;

                return (this.type().equals(that.type()) &&
                        this.get().equals(that.get()));
            }

            @Override
            public int hashCode() {
                int result = param.hashCode();
                result = 31 * result + arg.hashCode();

                return result;
            }
        }
    }
}