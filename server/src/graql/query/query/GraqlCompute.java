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

package grakn.core.graql.query.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.ConceptList;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.answer.ConceptSetMeasure;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.query.builder.Computable;
import graql.lang.util.Token;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static graql.lang.util.StringUtil.escapeLabelOrId;
import static java.util.stream.Collectors.joining;


/**
 * Graql Compute Query: to perform distributed analytics OLAP computation on Grakn
 *
 * @param <T> return type of ComputeQuery
 */
public class GraqlCompute<T extends Answer> extends GraqlQuery implements Computable {

    public final static Collection<Method> METHODS_ACCEPTED = ImmutableList.copyOf(Method.values());

    public final static Map<Method, Collection<Token.Compute.Condition>> CONDITIONS_REQUIRED = conditionsRequired();
    public final static Map<Method, Collection<Token.Compute.Condition>> CONDITIONS_OPTIONAL = conditionsOptional();
    public final static Map<Method, Collection<Token.Compute.Condition>> CONDITIONS_ACCEPTED = conditionsAccepted();

    public final static Map<Method, Token.Compute.Algorithm> ALGORITHMS_DEFAULT = algorithmsDefault();
    public final static Map<Method, Collection<Token.Compute.Algorithm>> ALGORITHMS_ACCEPTED = algorithmsAccepted();

    public final static Map<Method, Map<Token.Compute.Algorithm, Collection<Token.Compute.Param>>> ARGUMENTS_ACCEPTED = argumentsAccepted();
    public final static Map<Method, Map<Token.Compute.Algorithm, Map<Token.Compute.Param, Object>>> ARGUMENTS_DEFAULT = argumentsDefault();

    public final static Map<Method, Boolean> INCLUDE_ATTRIBUTES_DEFAULT = includeAttributesDefault();

    private Method method;
    boolean includeAttributes;

    // All these condition properties need to start off as NULL, they will be initialised when the user provides input
    ConceptId fromID = null;
    ConceptId toID = null;
    Set<String> ofTypes = null;
    Set<String> inTypes = null;
    Token.Compute.Algorithm algorithm = null;
    Arguments arguments = null; // But arguments will also be set when where() is called for cluster/centrality

    private final Map<Token.Compute.Condition, Supplier<Optional<?>>> conditionsMap = setConditionsMap();

    public GraqlCompute(Method method) {
        this(method, INCLUDE_ATTRIBUTES_DEFAULT.get(method));
    }

    public GraqlCompute(Method method, boolean includeAttributes) {
        this.method = method;
        this.includeAttributes = includeAttributes;
    }

    private static Map<Method, Collection<Token.Compute.Condition>> conditionsRequired() {
        Map<Method, Collection<Token.Compute.Condition>> required = new HashMap<>();
        required.put(Method.MIN, ImmutableSet.of(Token.Compute.Condition.OF));
        required.put(Method.MAX, ImmutableSet.of(Token.Compute.Condition.OF));
        required.put(Method.MEDIAN, ImmutableSet.of(Token.Compute.Condition.OF));
        required.put(Method.MEAN, ImmutableSet.of(Token.Compute.Condition.OF));
        required.put(Method.STD, ImmutableSet.of(Token.Compute.Condition.OF));
        required.put(Method.SUM, ImmutableSet.of(Token.Compute.Condition.OF));
        required.put(Method.PATH, ImmutableSet.of(Token.Compute.Condition.FROM, Token.Compute.Condition.TO));
        required.put(Method.CENTRALITY, ImmutableSet.of(Token.Compute.Condition.USING));
        required.put(Method.CLUSTER, ImmutableSet.of(Token.Compute.Condition.USING));

        return ImmutableMap.copyOf(required);
    }

    private static Map<Method, Collection<Token.Compute.Condition>> conditionsOptional() {
        Map<Method, Collection<Token.Compute.Condition>> optional = new HashMap<>();
        optional.put(Method.COUNT, ImmutableSet.of(Token.Compute.Condition.OF, Token.Compute.Condition.IN));
        optional.put(Method.MIN, ImmutableSet.of(Token.Compute.Condition.IN));
        optional.put(Method.MAX, ImmutableSet.of(Token.Compute.Condition.IN));
        optional.put(Method.MEDIAN, ImmutableSet.of(Token.Compute.Condition.IN));
        optional.put(Method.MEAN, ImmutableSet.of(Token.Compute.Condition.IN));
        optional.put(Method.STD, ImmutableSet.of(Token.Compute.Condition.IN));
        optional.put(Method.SUM, ImmutableSet.of(Token.Compute.Condition.IN));
        optional.put(Method.PATH, ImmutableSet.of(Token.Compute.Condition.IN));
        optional.put(Method.CENTRALITY, ImmutableSet.of(Token.Compute.Condition.OF, Token.Compute.Condition.IN, Token.Compute.Condition.WHERE));
        optional.put(Method.CLUSTER, ImmutableSet.of(Token.Compute.Condition.IN, Token.Compute.Condition.WHERE));

        return ImmutableMap.copyOf(optional);
    }

    private static Map<Method, Collection<Token.Compute.Condition>> conditionsAccepted() {
        Map<Method, Collection<Token.Compute.Condition>> accepted = new HashMap<>();

        for (Map.Entry<Method, Collection<Token.Compute.Condition>> entry : CONDITIONS_REQUIRED.entrySet()) {
            accepted.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        for (Map.Entry<Method, Collection<Token.Compute.Condition>> entry : CONDITIONS_OPTIONAL.entrySet()) {
            if (accepted.containsKey(entry.getKey())) accepted.get(entry.getKey()).addAll(entry.getValue());
            else accepted.put(entry.getKey(), entry.getValue());
        }

        return ImmutableMap.copyOf(accepted);
    }

    private static Map<Method, Collection<Token.Compute.Algorithm>> algorithmsAccepted() {
        Map<Method, Collection<Token.Compute.Algorithm>> accepted = new HashMap<>();

        accepted.put(Method.CENTRALITY, ImmutableSet.of(Token.Compute.Algorithm.DEGREE, Token.Compute.Algorithm.K_CORE));
        accepted.put(Method.CLUSTER, ImmutableSet.of(Token.Compute.Algorithm.CONNECTED_COMPONENT, Token.Compute.Algorithm.K_CORE));

        return ImmutableMap.copyOf(accepted);
    }

    private static Map<Method, Map<Token.Compute.Algorithm, Collection<Token.Compute.Param>>> argumentsAccepted() {
        Map<Method, Map<Token.Compute.Algorithm, Collection<Token.Compute.Param>>> accepted = new HashMap<>();

        accepted.put(Method.CENTRALITY, ImmutableMap.of(Token.Compute.Algorithm.K_CORE, ImmutableSet.of(Token.Compute.Param.MIN_K)));
        accepted.put(Method.CLUSTER, ImmutableMap.of(
                Token.Compute.Algorithm.K_CORE, ImmutableSet.of(Token.Compute.Param.K),
                Token.Compute.Algorithm.CONNECTED_COMPONENT, ImmutableSet.of(Token.Compute.Param.SIZE, Token.Compute.Param.CONTAINS)
        ));

        return ImmutableMap.copyOf(accepted);
    }

    private static Map<Method, Map<Token.Compute.Algorithm, Map<Token.Compute.Param, Object>>> argumentsDefault() {
        Map<Method, Map<Token.Compute.Algorithm, Map<Token.Compute.Param, Object>>> defaults = new HashMap<>();

        defaults.put(Method.CENTRALITY, ImmutableMap.of(Token.Compute.Algorithm.K_CORE, ImmutableMap.of(Token.Compute.Param.MIN_K, Argument.DEFAULT_MIN_K)));
        defaults.put(Method.CLUSTER, ImmutableMap.of(Token.Compute.Algorithm.K_CORE, ImmutableMap.of(Token.Compute.Param.K, Argument.DEFAULT_K)));

        return ImmutableMap.copyOf(defaults);
    }

    private static Map<Method, Token.Compute.Algorithm> algorithmsDefault() {
        Map<Method, Token.Compute.Algorithm> methodAlgorithm = new HashMap<>();
        methodAlgorithm.put(Method.CENTRALITY, Token.Compute.Algorithm.DEGREE);
        methodAlgorithm.put(Method.CLUSTER, Token.Compute.Algorithm.CONNECTED_COMPONENT);

        return ImmutableMap.copyOf(methodAlgorithm);
    }

    private static Map<Method, Boolean> includeAttributesDefault() {
        Map<Method, Boolean> map = new HashMap<>();
        map.put(Method.COUNT, false);
        map.put(Method.MIN, true);
        map.put(Method.MAX, true);
        map.put(Method.MEDIAN, true);
        map.put(Method.MEAN, true);
        map.put(Method.STD, true);
        map.put(Method.SUM, true);
        map.put(Method.PATH, false);
        map.put(Method.CENTRALITY, true);
        map.put(Method.CLUSTER, false);

        return ImmutableMap.copyOf(map);
    }

    private Map<Token.Compute.Condition, Supplier<Optional<?>>> setConditionsMap() {
        Map<Token.Compute.Condition, Supplier<Optional<?>>> conditions = new HashMap<>();
        conditions.put(Token.Compute.Condition.FROM, this::from);
        conditions.put(Token.Compute.Condition.TO, this::to);
        conditions.put(Token.Compute.Condition.OF, this::of);
        conditions.put(Token.Compute.Condition.IN, this::in);
        conditions.put(Token.Compute.Condition.USING, this::using);
        conditions.put(Token.Compute.Condition.WHERE, this::where);

        return conditions;
    }

    public final Method method() {
        return method;
    }

    @CheckReturnValue
    public final Optional<ConceptId> from() {
        return Optional.ofNullable(fromID);
    }

    @CheckReturnValue
    public final Optional<ConceptId> to() {
        return Optional.ofNullable(toID);
    }

    @CheckReturnValue
    public final Optional<Set<String>> of(){
        return Optional.ofNullable(ofTypes);
    }

    @CheckReturnValue
    public final Optional<Set<String>> in() {
        if (this.inTypes == null) return Optional.of(ImmutableSet.of());
        return Optional.of(this.inTypes);
    }

    @CheckReturnValue
    public final Optional<Token.Compute.Algorithm> using() {
        if (ALGORITHMS_DEFAULT.containsKey(method) && algorithm == null) {
            return Optional.of(ALGORITHMS_DEFAULT.get(method));
        }
        return Optional.ofNullable(algorithm);
    }

    @CheckReturnValue
    public final Optional<Arguments> where() {
        if (ARGUMENTS_DEFAULT.containsKey(method) && arguments == null) arguments = new Arguments();
        return Optional.ofNullable(this.arguments);
    }

    @CheckReturnValue
    public final boolean includesAttributes() {
        return includeAttributes;
    }

    @CheckReturnValue
    public final boolean isValid() {
        return !getException().isPresent();
    }

    @CheckReturnValue
    public Optional<GraqlQueryException> getException() {
        // Check that all required conditions for the current query method are provided
        for (Token.Compute.Condition condition : CONDITIONS_REQUIRED.getOrDefault(this.method(), Collections.emptyList())) {
            if (!this.conditionsMap.get(condition).get().isPresent()) {
                return Optional.of(GraqlQueryException.invalidComputeQuery_missingCondition(this.method()));
            }
        }

        // Check that all the provided conditions are accepted for the current query method
        for (Token.Compute.Condition condition : this.conditionsMap.keySet().stream()
                .filter(con -> this.conditionsMap.get(con).get().isPresent())
                .collect(Collectors.toSet())) {
            if (!CONDITIONS_ACCEPTED.get(this.method()).contains(condition)) {
                return Optional.of(GraqlQueryException.invalidComputeQuery_invalidCondition(this.method()));
            }
        }

        // Check that the provided algorithm is accepted for the current query method
        if (ALGORITHMS_ACCEPTED.containsKey(this.method()) && !ALGORITHMS_ACCEPTED.get(this.method()).contains(this.using().get())) {
            return Optional.of(GraqlQueryException.invalidComputeQuery_invalidMethodAlgorithm(this.method()));
        }

        // Check that the provided arguments are accepted for the current query method and algorithm
        if (this.where().isPresent()) {
            for (Token.Compute.Param param : this.where().get().getParameters()) {
                if (!ARGUMENTS_ACCEPTED.get(this.method()).get(this.using().get()).contains(param)) {
                    return Optional.of(GraqlQueryException.invalidComputeQuery_invalidArgument(this.method(), this.using().get()));
                }
            }
        }

        return Optional.empty();
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(Token.Command.COMPUTE).append(Token.Char.SPACE).append(method);
        if (!conditionsSyntax().isEmpty()) query.append(Token.Char.SPACE).append(conditionsSyntax());
        query.append(Token.Char.SEMICOLON);

        return query.toString();
    }

    private String conditionsSyntax() {
        List<String> conditionsList = new ArrayList<>();

        // It is important that we check for whether each condition is NULL, rather than using the getters.
        // Because, we want to know the user provided conditions, rather than the default conditions from the getters.
        // The exception is for arguments. It needs to be set internally for the query object to have default argument
        // values. However, we can query for .getParameters() to get user provided argument parameters.
        if (fromID != null) conditionsList.add(str(Token.Compute.Condition.FROM, Token.Char.SPACE, Token.Char.QUOTE, fromID, Token.Char.QUOTE));
        if (toID != null) conditionsList.add(str(Token.Compute.Condition.TO, Token.Char.SPACE, Token.Char.QUOTE, toID, Token.Char.QUOTE));
        if (ofTypes != null) conditionsList.add(ofSyntax());
        if (inTypes != null) conditionsList.add(inSyntax());
        if (algorithm != null) conditionsList.add(algorithmSyntax());
        if (arguments != null && !arguments.getParameters().isEmpty()) conditionsList.add(argumentsSyntax());

        return conditionsList.stream().collect(joining(Token.Char.COMMA_SPACE.toString()));
    }

    private String ofSyntax() {
        if (ofTypes != null) return str(Token.Compute.Condition.OF, Token.Char.SPACE, typesSyntax(ofTypes));

        return "";
    }

    private String inSyntax() {
        if (inTypes != null) return str(Token.Compute.Condition.IN, Token.Char.SPACE, typesSyntax(inTypes));

        return "";
    }

    private String typesSyntax(Set<String> types) {
        StringBuilder inTypesString = new StringBuilder();

        if (!types.isEmpty()) {
            if (types.size() == 1) {
                inTypesString.append(escapeLabelOrId(types.iterator().next()));
            } else {
                inTypesString.append(Token.Char.SQUARE_OPEN);
                inTypesString.append(inTypes.stream()
                                             .map(name -> escapeLabelOrId(name))
                                             .collect(joining(Token.Char.COMMA_SPACE.toString())));
                inTypesString.append(Token.Char.SQUARE_CLOSE);
            }
        }

        return inTypesString.toString();
    }

    private String algorithmSyntax() {
        if (algorithm != null) return str(Token.Compute.Condition.USING, Token.Char.SPACE, algorithm);

        return "";
    }

    private String argumentsSyntax() {
        if (arguments == null) return "";

        List<String> argumentsList = new ArrayList<>();
        StringBuilder argumentsString = new StringBuilder();

        for (Token.Compute.Param param : arguments.getParameters()) {
            argumentsList.add(str(param, Token.Comparator.EQ, arguments.getArgument(param).get()));
        }

        if (!argumentsList.isEmpty()) {
            argumentsString.append(str(Token.Compute.Condition.WHERE, Token.Char.SPACE));
            if (argumentsList.size() == 1) argumentsString.append(argumentsList.get(0));
            else {
                argumentsString.append(Token.Char.SQUARE_OPEN);
                argumentsString.append(argumentsList.stream().collect(joining(Token.Char.COMMA_SPACE.toString())));
                argumentsString.append(Token.Char.SQUARE_CLOSE);
            }
        }

        return argumentsString.toString();
    }

    private String str(Object... objects) {
        StringBuilder builder = new StringBuilder();
        for (Object obj : objects) builder.append(obj.toString());
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GraqlCompute<?> that = (GraqlCompute<?>) o;

        return (this.method().equals(that.method()) &&
                this.from().equals(that.from()) &&
                this.to().equals(that.to()) &&
                this.of().equals(that.of()) &&
                this.in().equals(that.in()) &&
                this.using().equals(that.using()) &&
                this.where().equals(that.where()) &&
                this.includesAttributes() == that.includesAttributes());
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(method);
        result = 31 * result + Objects.hashCode(fromID);
        result = 31 * result + Objects.hashCode(toID);
        result = 31 * result + Objects.hashCode(ofTypes);
        result = 31 * result + Objects.hashCode(inTypes);
        result = 31 * result + Objects.hashCode(algorithm);
        result = 31 * result + Objects.hashCode(arguments);
        result = 31 * result + Objects.hashCode(includeAttributes);

        return result;
    }

    public static class Builder {

        public GraqlCompute.Statistics.Count count() {
            return new GraqlCompute.Statistics.Count();
        }

        public GraqlCompute.Statistics.Value max() {
            return new GraqlCompute.Statistics.Value(Method.MAX);
        }

        public GraqlCompute.Statistics.Value min() {
            return new GraqlCompute.Statistics.Value(Method.MIN);
        }

        public GraqlCompute.Statistics.Value mean() {
            return new GraqlCompute.Statistics.Value(Method.MEAN);
        }

        public GraqlCompute.Statistics.Value median() {
            return new GraqlCompute.Statistics.Value(Method.MEDIAN);
        }

        public GraqlCompute.Statistics.Value sum() {
            return new GraqlCompute.Statistics.Value(Method.SUM);
        }

        public GraqlCompute.Statistics.Value std() {
            return new GraqlCompute.Statistics.Value(Method.STD);
        }

        public GraqlCompute.Path path() {
            return new GraqlCompute.Path();
        }

        public GraqlCompute.Centrality centrality() {
            return new GraqlCompute.Centrality();
        }

        public GraqlCompute.Cluster cluster() {
            return new GraqlCompute.Cluster();
        }

    }

    public static abstract class Statistics extends GraqlCompute<grakn.core.graql.answer.Value> {

        Statistics(Method method) {
            super(method);
        }

        public static class Count extends GraqlCompute<grakn.core.graql.answer.Value>
                implements Computable.Scopeable<GraqlCompute.Statistics.Count> {

            Count() {
                super(Method.COUNT);
            }

            @Override
            public GraqlCompute.Statistics.Count in(Collection<String> types) {
                this.inTypes = ImmutableSet.copyOf(types);
                return this;
            }

            @Override
            public GraqlCompute.Statistics.Count attributes(boolean include) {
                this.includeAttributes = include;
                return this;
            }
        }

        public static class Value extends GraqlCompute<grakn.core.graql.answer.Value>
                implements Computable.Targetable<Value>,
                           Computable.Scopeable<Value> {

            Value(Method method) {
                super(method);
            }

            @Override
            public GraqlCompute.Statistics.Value of(Collection<String> types) {
                this.ofTypes = ImmutableSet.copyOf(types);
                return this;
            }

            @Override
            public GraqlCompute.Statistics.Value in(Collection<String> types) {
                this.inTypes = ImmutableSet.copyOf(types);
                return this;
            }

            @Override
            public GraqlCompute.Statistics.Value attributes(boolean include) {
                this.includeAttributes = include;
                return this;
            }
        }
    }

    public static class Path extends GraqlCompute<ConceptList>
            implements Computable.Directional<GraqlCompute.Path>,
                       Computable.Scopeable<GraqlCompute.Path> {

        Path(){
            super(Method.PATH);
        }

        @Override
        public GraqlCompute.Path from(ConceptId fromID) {
            this.fromID = fromID;
            return this;
        }

        @Override
        public GraqlCompute.Path to(ConceptId toID) {
            this.toID = toID;
            return this;
        }

        @Override
        public GraqlCompute.Path in(Collection<String> types) {
            this.inTypes = ImmutableSet.copyOf(types);
            return this;
        }

        @Override
        public GraqlCompute.Path attributes(boolean include) {
            this.includeAttributes = include;
            return this;
        }
    }

    public static class Centrality extends GraqlCompute<ConceptSetMeasure>
            implements Computable.Targetable<GraqlCompute.Centrality>,
                       Computable.Scopeable<GraqlCompute.Centrality>,
                       Computable.Configurable<GraqlCompute.Centrality> {

        Centrality(){
            super(Method.CENTRALITY);
        }

        @Override
        public GraqlCompute.Centrality of(Collection<String> types) {
            this.ofTypes = ImmutableSet.copyOf(types);
            return this;
        }

        @Override
        public GraqlCompute.Centrality in(Collection<String> types) {
            this.inTypes = ImmutableSet.copyOf(types);
            return this;
        }

        @Override
        public GraqlCompute.Centrality attributes(boolean include) {
            this.includeAttributes = include;
            return this;
        }

        @Override
        public GraqlCompute.Centrality using(Token.Compute.Algorithm algorithm) {
            this.algorithm = algorithm;
            return this;
        }

        @Override
        public GraqlCompute.Centrality where(List<Argument> args) {
            if (this.arguments == null) this.arguments = new Arguments();
            for (Argument arg : args) this.arguments.setArgument(arg);

            return this;
        }
    }

    public static class Cluster extends GraqlCompute<ConceptSet>
            implements Computable.Scopeable<GraqlCompute.Cluster>,
                       Computable.Configurable<GraqlCompute.Cluster> {

        Cluster(){
            super(Method.CLUSTER);
        }

        @Override
        public GraqlCompute.Cluster in(Collection<String> types) {
            this.inTypes = ImmutableSet.copyOf(types);
            return this;
        }

        @Override
        public GraqlCompute.Cluster attributes(boolean include) {
            this.includeAttributes = include;
            return this;
        }

        @Override
        public GraqlCompute.Cluster using(Token.Compute.Algorithm algorithm) {
            this.algorithm = algorithm;
            return this;
        }

        @Override
        public GraqlCompute.Cluster where(List<Argument> args) {
            if (this.arguments == null) this.arguments = new Arguments();
            for (Argument arg : args) this.arguments.setArgument(arg);

            return this;
        }
    }

    public static class Method {
        public final static Method COUNT = new Method("count");
        public final static Method MIN = new Method("min");
        public final static Method MAX = new Method("max");
        public final static Method MEDIAN = new Method("median");
        public final static Method MEAN = new Method("mean");
        public final static Method STD = new Method("std");
        public final static Method SUM = new Method("sum");
        public final static Method PATH = new Method("path");
        public final static Method CENTRALITY = new Method("centrality");
        public final static Method CLUSTER = new Method("cluster");

        private final static List<Method> list = Arrays.asList(COUNT, MIN, MAX, MEDIAN, MEAN, STD, SUM, PATH, CENTRALITY, CLUSTER);
        private final String name;

        Method(String name) {
            this.name = name;
        }

        private static List<Method> values() { return list; }

        public String name() {
            return this.name;
        }

        @Override
        public String toString() {
            return this.name;
        }

        public static Method of(String name) {
            for (Method m : Method.values()) {
                if (m.name.equals(name)) {
                    return m;
                }
            }
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Method that = (Method) o;

            return (this.name().equals(that.name()));
        }

        @Override
        public int hashCode() {
            int result = 31 * name.hashCode();
            return result;
        }

    }

    /**
     * Graql Compute argument objects to be passed into the query
     *
     * @param <T>
     */
    public static class Argument<T> {

        public final static long DEFAULT_MIN_K = 2L;
        public final static long DEFAULT_K = 2L;

        private Token.Compute.Param param;
        private T arg;

        private Argument(Token.Compute.Param param, T arg) {
            this.param = param;
            this.arg = arg;
        }

        public final Token.Compute.Param type() {
            return this.param;
        }

        public final T get() {
            return this.arg;
        }

        public static Argument<Long> min_k(long minK) {
            return new Argument<>(Token.Compute.Param.MIN_K, minK);
        }

        public static Argument<Long> k(long k) {
            return new Argument<>(Token.Compute.Param.K, k);
        }

        public static Argument<Long> size(long size) {
            return new Argument<>(Token.Compute.Param.SIZE, size);
        }

        public static Argument<ConceptId> contains(ConceptId conceptId) {
            return new Argument<>(Token.Compute.Param.CONTAINS, conceptId);
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

    /**
     * Argument inner class to provide access Compute Query arguments
     */
    public class Arguments {

        private LinkedHashMap<Token.Compute.Param, Argument> argumentsOrdered = new LinkedHashMap<>();

        private final Map<Token.Compute.Param, Supplier<Optional<?>>> argumentsMap = setArgumentsMap();

        private Map<Token.Compute.Param, Supplier<Optional<?>>> setArgumentsMap() {
            Map<Token.Compute.Param, Supplier<Optional<?>>> arguments = new HashMap<>();
            arguments.put(Token.Compute.Param.MIN_K, this::minK);
            arguments.put(Token.Compute.Param.K, this::k);
            arguments.put(Token.Compute.Param.SIZE, this::size);
            arguments.put(Token.Compute.Param.CONTAINS, this::contains);

            return arguments;
        }

        private void setArgument(Argument arg) {
            argumentsOrdered.remove(arg.type());
            argumentsOrdered.put(arg.type(), arg);
        }

        @CheckReturnValue
        public Optional<?> getArgument(Token.Compute.Param param) {
            return argumentsMap.get(param).get();
        }

        @CheckReturnValue
        public Collection<Token.Compute.Param> getParameters() {
            return argumentsOrdered.keySet();
        }

        @CheckReturnValue
        public Optional<Long> minK() {
            Object defaultArg = getDefaultArgument(Token.Compute.Param.MIN_K);
            if (defaultArg != null) return Optional.of((Long) defaultArg);

            return Optional.ofNullable((Long) getArgumentValue(Token.Compute.Param.MIN_K));
        }

        @CheckReturnValue
        public Optional<Long> k() {
            Object defaultArg = getDefaultArgument(Token.Compute.Param.K);
            if (defaultArg != null) return Optional.of((Long) defaultArg);

            return Optional.ofNullable((Long) getArgumentValue(Token.Compute.Param.K));
        }

        @CheckReturnValue
        public Optional<Long> size() {
            return Optional.ofNullable((Long) getArgumentValue(Token.Compute.Param.SIZE));
        }

        @CheckReturnValue
        public Optional<ConceptId> contains() {
            return Optional.ofNullable((ConceptId) getArgumentValue(Token.Compute.Param.CONTAINS));
        }

        private Object getArgumentValue(Token.Compute.Param param) {
            return argumentsOrdered.get(param) != null ? argumentsOrdered.get(param).get() : null;
        }

        private Object getDefaultArgument(Token.Compute.Param param) {
            if (ARGUMENTS_DEFAULT.containsKey(method) &&
                    ARGUMENTS_DEFAULT.get(method).containsKey(algorithm) &&
                    ARGUMENTS_DEFAULT.get(method).get(algorithm).containsKey(param) &&
                    !argumentsOrdered.containsKey(param)) {
                return ARGUMENTS_DEFAULT.get(method).get(algorithm).get(param);
            }

            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Arguments that = (Arguments) o;

            return (this.minK().equals(that.minK()) &&
                    this.k().equals(that.k()) &&
                    this.size().equals(that.size()) &&
                    this.contains().equals(that.contains()));
        }


        @Override
        public int hashCode() {
            int h = 1;
            h *= 1000003;
            h ^= this.argumentsOrdered.hashCode();

            return h;
        }
    }
}
