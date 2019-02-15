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

import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.query.query.builder.Computable;
import graql.lang.exception.GraqlException;
import graql.lang.util.StringUtil;
import graql.lang.util.Token;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static graql.lang.util.Collections.map;
import static graql.lang.util.Collections.set;
import static graql.lang.util.Collections.tuple;
import static graql.lang.util.StringUtil.escapeLabelOrId;
import static java.util.stream.Collectors.joining;


/**
 * Graql Compute Query: to perform distributed analytics OLAP computation on Grakn
 */
public abstract class GraqlCompute extends GraqlQuery implements Computable {

    private Token.Compute.Method method;
    boolean includeAttributes;

    // All these condition properties need to start off as NULL,
    // they will be initialised when the user provides input
    ConceptId fromID = null;
    ConceptId toID = null;
    Set<String> ofTypes = null;
    Set<String> inTypes = null;
    Token.Compute.Algorithm algorithm = null;
    Arguments arguments = null;
    // But 'arguments' will also be set when where() is called for cluster/centrality

    private final Map<Token.Compute.Condition, Supplier<Optional<?>>> conditionsMap = setConditionsMap();

    protected GraqlCompute(Token.Compute.Method method, boolean includeAttributes) {
        this.method = method;
        this.includeAttributes = includeAttributes;
    }

    protected abstract Set<Token.Compute.Condition> conditionsRequired();

    private Map<Token.Compute.Method, Set<Token.Compute.Algorithm>> algorithmsAccepted() {
        Map<Token.Compute.Method, Set<Token.Compute.Algorithm>> accepted = new HashMap<>();

        accepted.put(Token.Compute.Method.CENTRALITY, set(Token.Compute.Algorithm.DEGREE, Token.Compute.Algorithm.K_CORE));
        accepted.put(Token.Compute.Method.CLUSTER, set(Token.Compute.Algorithm.CONNECTED_COMPONENT, Token.Compute.Algorithm.K_CORE));

        return Collections.unmodifiableMap(accepted);
    }

    private Map<Token.Compute.Method, Map<Token.Compute.Algorithm, Set<Token.Compute.Param>>> argumentsAccepted() {
        Map<Token.Compute.Method, Map<Token.Compute.Algorithm, Set<Token.Compute.Param>>> accepted = new HashMap<>();

        accepted.put(Token.Compute.Method.CENTRALITY, Collections.singletonMap(Token.Compute.Algorithm.K_CORE, Collections.singleton(Token.Compute.Param.MIN_K)));
        accepted.put(Token.Compute.Method.CLUSTER, map(
                tuple(Token.Compute.Algorithm.K_CORE, set(Token.Compute.Param.K)),
                tuple(Token.Compute.Algorithm.CONNECTED_COMPONENT, set(Token.Compute.Param.SIZE, Token.Compute.Param.CONTAINS))
        ));

        return Collections.unmodifiableMap(accepted);
    }

    private Map<Token.Compute.Method, Map<Token.Compute.Algorithm, Map<Token.Compute.Param, Object>>> argumentsDefault() {
        Map<Token.Compute.Method, Map<Token.Compute.Algorithm, Map<Token.Compute.Param, Object>>> defaults = new HashMap<>();

        defaults.put(Token.Compute.Method.CENTRALITY, map(tuple(Token.Compute.Algorithm.K_CORE, map(tuple(Token.Compute.Param.MIN_K, Argument.DEFAULT_MIN_K)))));
        defaults.put(Token.Compute.Method.CLUSTER, map(tuple(Token.Compute.Algorithm.K_CORE, map(tuple(Token.Compute.Param.K, Argument.DEFAULT_K)))));

        return Collections.unmodifiableMap(defaults);
    }

    private Map<Token.Compute.Method, Token.Compute.Algorithm> algorithmsDefault() {
        Map<Token.Compute.Method, Token.Compute.Algorithm> methodAlgorithm = new HashMap<>();
        methodAlgorithm.put(Token.Compute.Method.CENTRALITY, Token.Compute.Algorithm.DEGREE);
        methodAlgorithm.put(Token.Compute.Method.CLUSTER, Token.Compute.Algorithm.CONNECTED_COMPONENT);

        return Collections.unmodifiableMap(methodAlgorithm);
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

    @CheckReturnValue
    public final Token.Compute.Method method() {
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
        if (this.inTypes == null) return Optional.of(set());
        return Optional.of(this.inTypes);
    }

    @CheckReturnValue
    public final Optional<Token.Compute.Algorithm> using() {
        if (algorithmsDefault().containsKey(method) && algorithm == null) {
            return Optional.of(algorithmsDefault().get(method));
        }
        return Optional.ofNullable(algorithm);
    }

    @CheckReturnValue
    public final Optional<Arguments> where() {
        if (argumentsDefault().containsKey(method) && arguments == null) arguments = new Arguments();
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
    public Optional<GraqlException> getException() {
        // Check that all required conditions for the current query method are provided
        for (Token.Compute.Condition condition : conditionsRequired()) {
            if (!this.conditionsMap.get(condition).get().isPresent()) {
                return Optional.of(GraqlException.invalidComputeQuery_missingCondition(this.method(), conditionsRequired()));
            }
        }

        // Check that the provided algorithm is accepted for the current query method
        if (algorithmsAccepted().containsKey(this.method()) && !algorithmsAccepted().get(this.method()).contains(this.using().get())) {
            return Optional.of(GraqlException.invalidComputeQuery_invalidMethodAlgorithm(this.method(), algorithmsAccepted().get(this.method())));
        }

        // Check that the provided arguments are accepted for the current query method and algorithm
        if (this.where().isPresent()) {
            for (Token.Compute.Param param : this.where().get().getParameters()) {
                if (!argumentsAccepted().get(this.method()).get(this.using().get()).contains(param)) {
                    Token.Compute.Method method1 = this.method();
                    Token.Compute.Algorithm algorithm1 = this.using().get();
                    return Optional.of(GraqlException.invalidComputeQuery_invalidArgument(method1, algorithm1, argumentsAccepted().get(method1).get(algorithm1)));
                }
            }
        }

        return Optional.empty();
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(Token.Command.COMPUTE).append(Token.Char.SPACE).append(method);
        if (!printConditions().isEmpty()) query.append(Token.Char.SPACE).append(printConditions());
        query.append(Token.Char.SEMICOLON);

        return query.toString();
    }

    private String printConditions() {
        List<String> conditionsList = new ArrayList<>();

        // It is important that we check for whether each condition is NULL, rather than using the getters.
        // Because, we want to know the user provided conditions, rather than the default conditions from the getters.
        // The exception is for arguments. It needs to be set internally for the query object to have default argument
        // values. However, we can query for .getParameters() to get user provided argument parameters.
        if (fromID != null) conditionsList.add(str(Token.Compute.Condition.FROM, Token.Char.SPACE, Token.Char.QUOTE, fromID, Token.Char.QUOTE));
        if (toID != null) conditionsList.add(str(Token.Compute.Condition.TO, Token.Char.SPACE, Token.Char.QUOTE, toID, Token.Char.QUOTE));
        if (ofTypes != null) conditionsList.add(printOf());
        if (inTypes != null) conditionsList.add(printIn());
        if (algorithm != null) conditionsList.add(printAlgorithm());
        if (arguments != null && !arguments.getParameters().isEmpty()) conditionsList.add(printArguments());

        return conditionsList.stream().collect(joining(Token.Char.COMMA_SPACE.toString()));
    }

    private String printOf() {
        if (ofTypes != null) return str(Token.Compute.Condition.OF, Token.Char.SPACE, printTypes(ofTypes));

        return "";
    }

    private String printIn() {
        if (inTypes != null) return str(Token.Compute.Condition.IN, Token.Char.SPACE, printTypes(inTypes));

        return "";
    }

    private String printTypes(Set<String> types) {
        StringBuilder inTypesString = new StringBuilder();

        if (!types.isEmpty()) {
            if (types.size() == 1) {
                inTypesString.append(escapeLabelOrId(types.iterator().next()));
            } else {
                inTypesString.append(Token.Char.SQUARE_OPEN);
                inTypesString.append(inTypes.stream()
                                             .map(StringUtil::escapeLabelOrId)
                                             .collect(joining(Token.Char.COMMA_SPACE.toString())));
                inTypesString.append(Token.Char.SQUARE_CLOSE);
            }
        }

        return inTypesString.toString();
    }

    private String printAlgorithm() {
        if (algorithm != null) return str(Token.Compute.Condition.USING, Token.Char.SPACE, algorithm);

        return "";
    }

    private String printArguments() {
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

        GraqlCompute that = (GraqlCompute) o;

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
            return new GraqlCompute.Statistics.Value(Token.Compute.Method.MAX);
        }

        public GraqlCompute.Statistics.Value min() {
            return new GraqlCompute.Statistics.Value(Token.Compute.Method.MIN);
        }

        public GraqlCompute.Statistics.Value mean() {
            return new GraqlCompute.Statistics.Value(Token.Compute.Method.MEAN);
        }

        public GraqlCompute.Statistics.Value median() {
            return new GraqlCompute.Statistics.Value(Token.Compute.Method.MEDIAN);
        }

        public GraqlCompute.Statistics.Value sum() {
            return new GraqlCompute.Statistics.Value(Token.Compute.Method.SUM);
        }

        public GraqlCompute.Statistics.Value std() {
            return new GraqlCompute.Statistics.Value(Token.Compute.Method.STD);
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

    public static abstract class Statistics extends GraqlCompute {

        Statistics(Token.Compute.Method method, boolean includeAttributes) {
            super(method, includeAttributes);
        }

        public GraqlCompute.Statistics.Count asCount() {
            if (this instanceof GraqlCompute.Statistics.Count) {
                return (GraqlCompute.Statistics.Count) this;
            } else {
                throw GraqlException.create("This is not a GraqlCompute.Statistics.Count query");
            }
        }

        public GraqlCompute.Statistics.Value asValue() {
            if (this instanceof GraqlCompute.Statistics.Value) {
                return (GraqlCompute.Statistics.Value) this;
            } else {
                throw GraqlException.create("This is not a GraqlCompute.Statistics.Value query");
            }
        }

        public static class Count extends GraqlCompute.Statistics
                implements Computable.Scopeable<GraqlCompute.Statistics.Count> {

            Count() {
                super(Token.Compute.Method.COUNT, false);
            }

            @Override
            public GraqlCompute.Statistics.Count in(Collection<String> types) {
                this.inTypes = set(types);
                return this;
            }

            @Override
            public GraqlCompute.Statistics.Count attributes(boolean include) {
                this.includeAttributes = include;
                return this;
            }

            protected Set<Token.Compute.Condition> conditionsRequired() {
                return set();
            }
        }

        public static class Value extends GraqlCompute.Statistics
                implements Computable.Targetable<Value>,
                           Computable.Scopeable<Value> {

            Value(Token.Compute.Method method) {
                super(method, true);
            }

            @Override
            public GraqlCompute.Statistics.Value of(Collection<String> types) {
                this.ofTypes = set(types);
                return this;
            }

            @Override
            public GraqlCompute.Statistics.Value in(Collection<String> types) {
                this.inTypes = set(types);
                return this;
            }

            @Override
            public GraqlCompute.Statistics.Value attributes(boolean include) {
                this.includeAttributes = include;
                return this;
            }

            protected Set<Token.Compute.Condition> conditionsRequired() {
                return set(Token.Compute.Condition.OF);
            }
        }
    }

    public static class Path extends GraqlCompute
            implements Computable.Directional<GraqlCompute.Path>,
                       Computable.Scopeable<GraqlCompute.Path> {

        Path(){
            super(Token.Compute.Method.PATH, false);
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
            this.inTypes = set(types);
            return this;
        }

        @Override
        public GraqlCompute.Path attributes(boolean include) {
            this.includeAttributes = include;
            return this;
        }

        protected Set<Token.Compute.Condition> conditionsRequired() {
            return set(Token.Compute.Condition.FROM, Token.Compute.Condition.TO);
        }
    }

    public static class Centrality extends GraqlCompute
            implements Computable.Targetable<GraqlCompute.Centrality>,
                       Computable.Scopeable<GraqlCompute.Centrality>,
                       Computable.Configurable<GraqlCompute.Centrality> {

        Centrality(){
            super(Token.Compute.Method.CENTRALITY, true);
        }

        @Override
        public GraqlCompute.Centrality of(Collection<String> types) {
            this.ofTypes = set(types);
            return this;
        }

        @Override
        public GraqlCompute.Centrality in(Collection<String> types) {
            this.inTypes = set(types);
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

        protected Set<Token.Compute.Condition> conditionsRequired() {
            return set(Token.Compute.Condition.USING);
        }
    }

    public static class Cluster extends GraqlCompute
            implements Computable.Scopeable<GraqlCompute.Cluster>,
                       Computable.Configurable<GraqlCompute.Cluster> {

        Cluster(){
            super(Token.Compute.Method.CLUSTER, false);
        }

        @Override
        public GraqlCompute.Cluster in(Collection<String> types) {
            this.inTypes = set(types);
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

        protected Set<Token.Compute.Condition> conditionsRequired() {
            return set(Token.Compute.Condition.USING);
        }
    }

    /**
     * Graql Compute argument objects to be passed into the query
     *
     * @param <T>
     */
    public static class Argument<T> {

        final static long DEFAULT_MIN_K = 2L;
        final static long DEFAULT_K = 2L;

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
        public Set<Token.Compute.Param> getParameters() {
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
            if (argumentsDefault().containsKey(method) &&
                    argumentsDefault().get(method).containsKey(algorithm) &&
                    argumentsDefault().get(method).get(algorithm).containsKey(param) &&
                    !argumentsOrdered.containsKey(param)) {
                return argumentsDefault().get(method).get(algorithm).get(param);
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
