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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.answer.Answer;
import grakn.core.graql.answer.ConceptList;
import grakn.core.graql.answer.ConceptSet;
import grakn.core.graql.answer.ConceptSetMeasure;
import grakn.core.graql.answer.Value;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.util.StringUtil;
import grakn.core.server.ComputeExecutor;
import grakn.core.server.Transaction;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static grakn.core.common.util.CommonUtil.toImmutableSet;
import static java.util.stream.Collectors.joining;


/**
 * Graql Compute Query: to perform distributed analytics OLAP computation on Grakn
 *
 * @param <T> return type of ComputeQuery
 */
public class ComputeQuery<T extends Answer> implements Query<T> {

    public final static Collection<Method> METHODS_ACCEPTED = ImmutableList.copyOf(Method.values());

    public final static Map<Method, Collection<Condition>> CONDITIONS_REQUIRED = conditionsRequired();
    public final static Map<Method, Collection<Condition>> CONDITIONS_OPTIONAL = conditionsOptional();
    public final static Map<Method, Collection<Condition>> CONDITIONS_ACCEPTED = conditionsAccepted();

    public final static Map<Method, Algorithm> ALGORITHMS_DEFAULT = algorithmsDefault();
    public final static Map<Method, Collection<Algorithm>> ALGORITHMS_ACCEPTED = algorithmsAccepted();

    public final static Map<Method, Map<Algorithm, Collection<Param>>> ARGUMENTS_ACCEPTED = argumentsAccepted();
    public final static Map<Method, Map<Algorithm, Map<Param, Object>>> ARGUMENTS_DEFAULT = argumentsDefault();

    public final static Map<Method, Boolean> INCLUDE_ATTRIBUTES_DEFAULT = includeAttributesDefault();

    private Transaction tx;
    private Set<ComputeExecutor> runningJobs = ConcurrentHashMap.newKeySet();

    private Method method;
    private boolean includeAttributes;

    // All these condition properties need to start off as NULL, they will be initialised when the user provides input
    private ConceptId fromID = null;
    private ConceptId toID = null;
    private Set<Label> ofTypes = null;
    private Set<Label> inTypes = null;
    private Algorithm algorithm = null;
    private Arguments arguments = null; // But arguments will also be set when where() is called for cluster/centrality

    private final Map<Condition, Supplier<Optional<?>>> conditionsMap = setConditionsMap();

    public ComputeQuery(Transaction tx, Method<T> method) {
        this(tx, method, INCLUDE_ATTRIBUTES_DEFAULT.get(method));
    }

    public ComputeQuery(Transaction tx, Method method, boolean includeAttributes) {
        this.method = method;
        this.tx = tx;
        this.includeAttributes = includeAttributes;
    }

    private static Map<Method, Collection<Condition>> conditionsRequired() {
        Map<Method, Collection<Condition>> required = new HashMap<>();
        required.put(Method.MIN, ImmutableSet.of(Condition.OF));
        required.put(Method.MAX, ImmutableSet.of(Condition.OF));
        required.put(Method.MEDIAN, ImmutableSet.of(Condition.OF));
        required.put(Method.MEAN, ImmutableSet.of(Condition.OF));
        required.put(Method.STD, ImmutableSet.of(Condition.OF));
        required.put(Method.SUM, ImmutableSet.of(Condition.OF));
        required.put(Method.PATH, ImmutableSet.of(Condition.FROM, Condition.TO));
        required.put(Method.CENTRALITY, ImmutableSet.of(Condition.USING));
        ;
        required.put(Method.CLUSTER, ImmutableSet.of(Condition.USING));

        return ImmutableMap.copyOf(required);
    }

    private static Map<Method, Collection<Condition>> conditionsOptional() {
        Map<Method, Collection<Condition>> optional = new HashMap<>();
        optional.put(Method.COUNT, ImmutableSet.of(Condition.IN));
        optional.put(Method.MIN, ImmutableSet.of(Condition.IN));
        optional.put(Method.MAX, ImmutableSet.of(Condition.IN));
        optional.put(Method.MEDIAN, ImmutableSet.of(Condition.IN));
        optional.put(Method.MEAN, ImmutableSet.of(Condition.IN));
        optional.put(Method.STD, ImmutableSet.of(Condition.IN));
        optional.put(Method.SUM, ImmutableSet.of(Condition.IN));
        optional.put(Method.PATH, ImmutableSet.of(Condition.IN));
        optional.put(Method.CENTRALITY, ImmutableSet.of(Condition.OF, Condition.IN, Condition.WHERE));
        optional.put(Method.CLUSTER, ImmutableSet.of(Condition.IN, Condition.WHERE));

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

        accepted.put(Method.CENTRALITY, ImmutableSet.of(Algorithm.DEGREE, Algorithm.K_CORE));
        accepted.put(Method.CLUSTER, ImmutableSet.of(Algorithm.CONNECTED_COMPONENT, Algorithm.K_CORE));

        return ImmutableMap.copyOf(accepted);
    }

    private static Map<Method, Map<Algorithm, Collection<Param>>> argumentsAccepted() {
        Map<Method, Map<Algorithm, Collection<Param>>> accepted = new HashMap<>();

        accepted.put(Method.CENTRALITY, ImmutableMap.of(Algorithm.K_CORE, ImmutableSet.of(Param.MIN_K)));
        accepted.put(Method.CLUSTER, ImmutableMap.of(
                Algorithm.K_CORE, ImmutableSet.of(Param.K),
                Algorithm.CONNECTED_COMPONENT, ImmutableSet.of(Param.SIZE, Param.CONTAINS)
        ));

        return ImmutableMap.copyOf(accepted);
    }

    private static Map<Method, Map<Algorithm, Map<Param, Object>>> argumentsDefault() {
        Map<Method, Map<Algorithm, Map<Param, Object>>> defaults = new HashMap<>();

        defaults.put(Method.CENTRALITY, ImmutableMap.of(Algorithm.K_CORE, ImmutableMap.of(Param.MIN_K, Argument.DEFAULT_MIN_K)));
        defaults.put(Method.CLUSTER, ImmutableMap.of(Algorithm.K_CORE, ImmutableMap.of(Param.K, Argument.DEFAULT_K)));

        return ImmutableMap.copyOf(defaults);
    }

    private static Map<Method, Algorithm> algorithmsDefault() {
        Map<Method, Algorithm> methodAlgorithm = new HashMap<>();
        methodAlgorithm.put(Method.CENTRALITY, Algorithm.DEGREE);
        methodAlgorithm.put(Method.CLUSTER, Algorithm.CONNECTED_COMPONENT);

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

    private Map<Condition, Supplier<Optional<?>>> setConditionsMap() {
        Map<Condition, Supplier<Optional<?>>> conditions = new HashMap<>();
        conditions.put(Condition.FROM, this::from);
        conditions.put(Condition.TO, this::to);
        conditions.put(Condition.OF, this::of);
        conditions.put(Condition.IN, this::in);
        conditions.put(Condition.USING, this::using);
        conditions.put(Condition.WHERE, this::where);

        return conditions;
    }

    @Override
    public Stream<T> stream(boolean infer) {
        return stream();
    }

    @Override
    public Stream<T> stream() {
        Optional<GraqlQueryException> exception = getException();
        if (exception.isPresent()) throw exception.get();

        ComputeExecutor<T> job = executor().run(this);

        runningJobs.add(job);

        try {
            return job.stream();
        } finally {
            runningJobs.remove(job);
        }
    }

    public final void kill() {
        runningJobs.forEach(ComputeExecutor::kill);
    }

    @Override
    public final Transaction tx() {
        return tx;
    }

    public final Method method() {
        return method;
    }

    public final ComputeQuery<T> from(ConceptId fromID) {
        this.fromID = fromID;
        return this;
    }

    @CheckReturnValue
    public final Optional<ConceptId> from() {
        return Optional.ofNullable(fromID);
    }

    public final ComputeQuery<T> to(ConceptId toID) {
        this.toID = toID;
        return this;
    }

    @CheckReturnValue
    public final Optional<ConceptId> to() {
        return Optional.ofNullable(toID);
    }

    public final ComputeQuery<T> of(String type, String... types) {
        ArrayList<String> typeList = new ArrayList<>(types.length + 1);
        typeList.add(type);
        typeList.addAll(Arrays.asList(types));

        return of(typeList.stream().map(Label::of).collect(toImmutableSet()));
    }

    public final ComputeQuery<T> of(Collection<Label> types) {
        this.ofTypes = ImmutableSet.copyOf(types);

        return this;
    }

    @CheckReturnValue
    public final Optional<Set<Label>> of() {
        return Optional.ofNullable(ofTypes);
    }

    public final ComputeQuery<T> in(String type, String... types) {
        ArrayList<String> typeList = new ArrayList<>(types.length + 1);
        typeList.add(type);
        typeList.addAll(Arrays.asList(types));

        return in(typeList.stream().map(Label::of).collect(toImmutableSet()));
    }

    public final ComputeQuery<T> in(Collection<Label> types) {
        this.inTypes = ImmutableSet.copyOf(types);
        return this;
    }

    @CheckReturnValue
    public final Optional<Set<Label>> in() {
        if (this.inTypes == null) return Optional.of(ImmutableSet.of());
        return Optional.of(this.inTypes);
    }

    public final ComputeQuery<T> using(Algorithm algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    @CheckReturnValue
    public final Optional<Algorithm> using() {
        if (ALGORITHMS_DEFAULT.containsKey(method) && algorithm == null)
            return Optional.of(ALGORITHMS_DEFAULT.get(method));
        return Optional.ofNullable(algorithm);
    }

    public final ComputeQuery<T> where(Argument arg, Argument... args) {
        ArrayList<Argument> argList = new ArrayList(args.length + 1);
        argList.add(arg);
        argList.addAll(Arrays.asList(args));

        return this.where(argList);
    }

    public final ComputeQuery<T> where(Collection<Argument> args) {
        if (this.arguments == null) this.arguments = new Arguments();
        for (Argument arg : args) this.arguments.setArgument(arg);

        return this;
    }

    @CheckReturnValue
    public final Optional<Arguments> where() {
        if (ARGUMENTS_DEFAULT.containsKey(method) && arguments == null) arguments = new Arguments();
        return Optional.ofNullable(this.arguments);
    }

    public final ComputeQuery<T> includeAttributes(boolean include) {
        this.includeAttributes = include;
        return this;
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
        for (Condition condition : collect(CONDITIONS_REQUIRED.get(this.method()))) {
            if (!this.conditionsMap.get(condition).get().isPresent()) {
                return Optional.of(GraqlQueryException.invalidComputeQuery_missingCondition(this.method()));
            }
        }

        // Check that all the provided conditions are accepted for the current query method
        for (Condition condition : this.conditionsMap.keySet().stream()
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
            for (Param param : this.where().get().getParameters()) {
                if (!ARGUMENTS_ACCEPTED.get(this.method()).get(this.using().get()).contains(param)) {
                    return Optional.of(GraqlQueryException.invalidComputeQuery_invalidArgument(this.method(), this.using().get()));
                }
            }
        }

        return Optional.empty();
    }

    private <T> Collection<T> collect(Collection<T> collection) {
        return collection != null ? collection : Collections.emptyList();
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(str(Command.COMPUTE, Char.SPACE, method));
        if (!conditionsSyntax().isEmpty()) query.append(str(Char.SPACE, conditionsSyntax()));
        query.append(Char.SEMICOLON);

        return query.toString();
    }

    private String conditionsSyntax() {
        List<String> conditionsList = new ArrayList<>();

        // It is important that check for whether each condition is NULL, rather than using the getters.
        // Because, we want to know the user provided conditions, rather than the default conditions from the getters.
        // The exception is for arguments. It needs to be set internally for the query object to have default argument
        // values. However, we can query for .getParameters() to get user provided argument parameters.
        if (fromID != null) conditionsList.add(str(Condition.FROM, Char.SPACE, Char.QUOTE, fromID, Char.QUOTE));
        if (toID != null) conditionsList.add(str(Condition.TO, Char.SPACE, Char.QUOTE, toID, Char.QUOTE));
        if (ofTypes != null) conditionsList.add(ofSyntax());
        if (inTypes != null) conditionsList.add(inSyntax());
        if (algorithm != null) conditionsList.add(algorithmSyntax());
        if (arguments != null && !arguments.getParameters().isEmpty()) conditionsList.add(argumentsSyntax());

        return conditionsList.stream().collect(joining(Char.COMMA_SPACE.toString()));
    }

    private String ofSyntax() {
        if (ofTypes != null) return str(Condition.OF, Char.SPACE, typesSyntax(ofTypes));

        return "";
    }

    private String inSyntax() {
        if (inTypes != null) return str(Condition.IN, Char.SPACE, typesSyntax(inTypes));

        return "";
    }

    private String typesSyntax(Set<Label> types) {
        StringBuilder inTypesString = new StringBuilder();

        if (!types.isEmpty()) {
            if (types.size() == 1) inTypesString.append(StringUtil.typeLabelToString(types.iterator().next()));
            else {
                inTypesString.append(Char.SQUARE_OPEN);
                inTypesString.append(inTypes.stream().map(StringUtil::typeLabelToString).collect(joining(Char.COMMA_SPACE.toString())));
                inTypesString.append(Char.SQUARE_CLOSE);
            }
        }

        return inTypesString.toString();
    }

    private String algorithmSyntax() {
        if (algorithm != null) return str(Condition.USING, Char.SPACE, algorithm);

        return "";
    }

    private String argumentsSyntax() {
        if (arguments == null) return "";

        List<String> argumentsList = new ArrayList<>();
        StringBuilder argumentsString = new StringBuilder();

        for (Param param : arguments.getParameters()) {
            argumentsList.add(str(param, Char.EQUAL, arguments.getArgument(param).get()));
        }

        if (!argumentsList.isEmpty()) {
            argumentsString.append(str(Condition.WHERE, Char.SPACE));
            if (argumentsList.size() == 1) argumentsString.append(argumentsList.get(0));
            else {
                argumentsString.append(Char.SQUARE_OPEN);
                argumentsString.append(argumentsList.stream().collect(joining(Char.COMMA_SPACE.toString())));
                argumentsString.append(Char.SQUARE_CLOSE);
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

        ComputeQuery<?> that = (ComputeQuery<?>) o;

        return (Objects.equals(this.tx(), that.tx()) &&
                this.method().equals(that.method()) &&
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
        int result = tx.hashCode();
        result = 31 * result + Objects.hashCode(method);
        result = 31 * result + Objects.hashCode(fromID);
        result = 31 * result + Objects.hashCode(toID);
        result = 31 * result + Objects.hashCode(ofTypes);
        result = 31 * result + Objects.hashCode(inTypes);
        result = 31 * result + Objects.hashCode(algorithm);
        result = 31 * result + Objects.hashCode(arguments);
        result = 31 * result + Objects.hashCode(includeAttributes);

        return result;
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

    /**
     * Graql compute method types to determine the type of calculation to execute
     *
     * @param <T> return type of ComputeQuery
     */
    public static class Method<T extends Answer> {
        public final static Method<Value> COUNT = new Method<>("count");
        public final static Method<Value> MIN = new Method<>("min");
        public final static Method<Value> MAX = new Method<>("max");
        public final static Method<Value> MEDIAN = new Method<>("median");
        public final static Method<Value> MEAN = new Method<>("mean");
        public final static Method<Value> STD = new Method<>("std");
        public final static Method<Value> SUM = new Method<>("sum");
        public final static Method<ConceptList> PATH = new Method<>("path");
        public final static Method<ConceptSetMeasure> CENTRALITY = new Method<>("centrality");
        public final static Method<ConceptSet> CLUSTER = new Method<>("cluster");

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

        public static Method<?> of(String name) {
            for (Method<?> m : Method.values()) {
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

            Method<?> that = (Method<?>) o;

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

        private Param param;
        private T arg;

        private Argument(Param param, T arg) {
            this.param = param;
            this.arg = arg;
        }

        public final Param type() {
            return this.param;
        }

        public final T get() {
            return this.arg;
        }

        public static Argument<Long> min_k(long minK) {
            return new Argument<>(Param.MIN_K, minK);
        }

        public static Argument<Long> k(long k) {
            return new Argument<>(Param.K, k);
        }

        public static Argument<Long> size(long size) {
            return new Argument<>(Param.SIZE, size);
        }

        public static Argument<ConceptId> contains(ConceptId conceptId) {
            return new Argument<>(Param.CONTAINS, conceptId);
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

        private LinkedHashMap<Param, Argument> argumentsOrdered = new LinkedHashMap<>();

        private final Map<Param, Supplier<Optional<?>>> argumentsMap = setArgumentsMap();

        private Map<Param, Supplier<Optional<?>>> setArgumentsMap() {
            Map<Param, Supplier<Optional<?>>> arguments = new HashMap<>();
            arguments.put(Param.MIN_K, this::minK);
            arguments.put(Param.K, this::k);
            arguments.put(Param.SIZE, this::size);
            arguments.put(Param.CONTAINS, this::contains);

            return arguments;
        }

        private void setArgument(Argument arg) {
            argumentsOrdered.remove(arg.type());
            argumentsOrdered.put(arg.type(), arg);
        }

        @CheckReturnValue
        public Optional<?> getArgument(Param param) {
            return argumentsMap.get(param).get();
        }

        @CheckReturnValue
        public Collection<Param> getParameters() {
            return argumentsOrdered.keySet();
        }

        @CheckReturnValue
        public Optional<Long> minK() {
            Object defaultArg = getDefaultArgument(Param.MIN_K);
            if (defaultArg != null) return Optional.of((Long) defaultArg);

            return Optional.ofNullable((Long) getArgumentValue(Param.MIN_K));
        }

        @CheckReturnValue
        public Optional<Long> k() {
            Object defaultArg = getDefaultArgument(Param.K);
            if (defaultArg != null) return Optional.of((Long) defaultArg);

            return Optional.ofNullable((Long) getArgumentValue(Param.K));
        }

        @CheckReturnValue
        public Optional<Long> size() {
            return Optional.ofNullable((Long) getArgumentValue(Param.SIZE));
        }

        @CheckReturnValue
        public Optional<ConceptId> contains() {
            return Optional.ofNullable((ConceptId) getArgumentValue(Param.CONTAINS));
        }

        private Object getArgumentValue(Param param) {
            return argumentsOrdered.get(param) != null ? argumentsOrdered.get(param).get() : null;
        }

        private Object getDefaultArgument(Param param) {
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
            int result = tx.hashCode();
            result = 31 * result + argumentsOrdered.hashCode();

            return result;
        }
    }
}
