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

import grakn.core.graql.query.query.builder.Computable;
import graql.lang.exception.GraqlException;
import graql.lang.util.StringUtil;
import graql.lang.util.Token;

import javax.annotation.CheckReturnValue;
import java.util.ArrayList;
import java.util.Collection;
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
    String fromID = null;
    String toID = null;
    Set<String> ofTypes = null;
    Set<String> inTypes = null;
    Token.Compute.Algorithm algorithm = null;
    Arguments arguments = null;
    // But 'arguments' will also be set when where() is called for cluster/centrality

    protected GraqlCompute(Token.Compute.Method method, boolean includeAttributes) {
        this.method = method;
        this.includeAttributes = includeAttributes;
    }

    @CheckReturnValue
    public final Token.Compute.Method method() {
        return method;
    }

    @CheckReturnValue
    public Set<String> in() {
        if (this.inTypes == null) return set();
        return inTypes;
    }

    @CheckReturnValue
    public boolean includesAttributes() {
        return includeAttributes;
    }

    @CheckReturnValue
    public final boolean isValid() {
        return !getException().isPresent();
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

            @Override
            public Set<Token.Compute.Condition> conditionsRequired() {
                return set();
            }

            @Override
            public Optional<GraqlException> getException() {
                return Optional.empty();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                GraqlCompute.Statistics.Count that = (GraqlCompute.Statistics.Count) o;

                return (this.method().equals(that.method()) &&
                        this.in().equals(that.in()) &&
                        this.includesAttributes() == that.includesAttributes());
            }

            @Override
            public int hashCode() {
                int result = Objects.hashCode(method());
                result = 31 * result + Objects.hashCode(in());
                result = 31 * result + Objects.hashCode(includesAttributes());

                return result;
            }
        }

        public static class Value extends GraqlCompute.Statistics
                implements Computable.Targetable<Value>,
                           Computable.Scopeable<Value> {

            Value(Token.Compute.Method method) {
                super(method, true);
            }

            @CheckReturnValue
            public final Set<String> of(){
                return ofTypes == null ? set() : ofTypes;
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

            @Override
            public Set<Token.Compute.Condition> conditionsRequired() {
                return set(Token.Compute.Condition.OF);
            }

            @Override
            public Optional<GraqlException> getException() {
                if (ofTypes == null) {
                    return Optional.of(GraqlException.invalidComputeQuery_missingCondition(
                            this.method(), conditionsRequired()
                    ));
                } else {
                    return Optional.empty();
                }
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                GraqlCompute.Statistics.Value that = (GraqlCompute.Statistics.Value) o;

                return (this.method().equals(that.method()) &&
                        this.of().equals(that.of()) &&
                        this.in().equals(that.in()) &&
                        this.includesAttributes() == that.includesAttributes());
            }

            @Override
            public int hashCode() {
                int result = Objects.hashCode(method());
                result = 31 * result + Objects.hashCode(of());
                result = 31 * result + Objects.hashCode(in());
                result = 31 * result + Objects.hashCode(includesAttributes());

                return result;
            }
        }
    }

    public static class Path extends GraqlCompute
            implements Computable.Directional<GraqlCompute.Path>,
                       Computable.Scopeable<GraqlCompute.Path> {

        Path(){
            super(Token.Compute.Method.PATH, false);
        }

        @CheckReturnValue
        public final String from() {
            return fromID;
        }

        @CheckReturnValue
        public final String to() {
            return toID;
        }

        @Override
        public GraqlCompute.Path from(String fromID) {
            this.fromID = fromID;
            return this;
        }

        @Override
        public GraqlCompute.Path to(String toID) {
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

        @Override
        public Set<Token.Compute.Condition> conditionsRequired() {
            return set(Token.Compute.Condition.FROM, Token.Compute.Condition.TO);
        }

        @Override
        public  Optional<GraqlException> getException() {
            if (fromID == null || toID == null) {
                return Optional.of(GraqlException.invalidComputeQuery_missingCondition(
                        this.method(), conditionsRequired()
                ));
            } else {
                return Optional.empty();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraqlCompute.Path that = (GraqlCompute.Path) o;

            return (this.method().equals(that.method()) &&
                    this.from().equals(that.from()) &&
                    this.to().equals(that.to()) &&
                    this.in().equals(that.in()) &&
                    this.includesAttributes() == that.includesAttributes());
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(method());
            result = 31 * result + Objects.hashCode(from());
            result = 31 * result + Objects.hashCode(to());
            result = 31 * result + Objects.hashCode(in());
            result = 31 * result + Objects.hashCode(includesAttributes());

            return result;
        }
    }

    private static abstract class Configurable<T extends Configurable> extends GraqlCompute
            implements Computable.Scopeable<T>,
                       Computable.Configurable<T, GraqlCompute.Argument, GraqlCompute.Arguments> {

        Configurable(Token.Compute.Method method, boolean includeAttributes) {
            super(method, includeAttributes);
        }

        protected abstract T self();

        @CheckReturnValue
        public GraqlCompute.Arguments where() {
            GraqlCompute.Arguments args = arguments;
            if (args == null) {
                args = new GraqlCompute.Arguments();
            }
            if (argumentsDefault().containsKey(using())) {
                args.setDefaults(argumentsDefault().get(using()));
            }
            return args;
        }


        @CheckReturnValue
        public Token.Compute.Algorithm using() {
            if (algorithm == null) {
                return Token.Compute.Algorithm.DEGREE;
            } else {
                return algorithm;
            }
        }

        @Override
        public T in(Collection<String> types) {
            this.inTypes = set(types);
            return self();
        }

        @Override
        public T attributes(boolean include) {
            this.includeAttributes = include;
            return self();
        }

        @Override
        public T using(Token.Compute.Algorithm algorithm) {
            this.algorithm = algorithm;
            return self();
        }

        @Override
        public T where(List<GraqlCompute.Argument> args) {
            if (this.arguments == null) this.arguments = new GraqlCompute.Arguments();
            for (GraqlCompute.Argument<?> arg : args) this.arguments.setArgument(arg);
            return self();
        }


        @Override
        public Set<Token.Compute.Condition> conditionsRequired() {
            return set(Token.Compute.Condition.USING);
        }

        @Override
        public Optional<GraqlException> getException() {
            if (!algorithmsAccepted().contains(using())) {
                return Optional.of(GraqlException.invalidComputeQuery_invalidMethodAlgorithm(method(), algorithmsAccepted()));
            }

            // Check that the provided arguments are accepted for the current query method and algorithm
            for (Token.Compute.Param param : this.where().getParameters()) {
                if (!argumentsAccepted().get(this.using()).contains(param)) {
                    return Optional.of(GraqlException.invalidComputeQuery_invalidArgument(
                            this.method(), this.using(), argumentsAccepted().get(this.using())
                    ));
                }
            }

            return Optional.empty();
        }
    }

    public static class Centrality extends GraqlCompute.Configurable<GraqlCompute.Centrality>
            implements Computable.Targetable<GraqlCompute.Centrality> {

        final static long DEFAULT_MIN_K = 2L;

        Centrality(){
            super(Token.Compute.Method.CENTRALITY, true);
        }

        protected GraqlCompute.Centrality self() {
            return this;
        }

        @CheckReturnValue
        public final Set<String> of(){
            return ofTypes == null ? set() : ofTypes;
        }

        @Override
        public GraqlCompute.Centrality of(Collection<String> types) {
            this.ofTypes = set(types);
            return this;
        }

        @Override
        public Set<Token.Compute.Algorithm> algorithmsAccepted() {
            return set(Token.Compute.Algorithm.DEGREE, Token.Compute.Algorithm.K_CORE);
        }

        @Override
        public Map<Token.Compute.Algorithm, Set<Token.Compute.Param>> argumentsAccepted() {
            return map(tuple(Token.Compute.Algorithm.K_CORE, set(Token.Compute.Param.MIN_K)));
        }

        @Override
        public Map<Token.Compute.Algorithm, Map<Token.Compute.Param, Object>> argumentsDefault() {
            return map(tuple(Token.Compute.Algorithm.K_CORE, map(tuple(Token.Compute.Param.MIN_K, DEFAULT_MIN_K))));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraqlCompute.Centrality that = (GraqlCompute.Centrality) o;

            return (this.method().equals(that.method()) &&
                    this.of().equals(that.of()) &&
                    this.in().equals(that.in()) &&
                    this.using().equals(that.using()) &&
                    this.where().equals(that.where()) &&
                    this.includesAttributes() == that.includesAttributes());
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(method());
            result = 31 * result + Objects.hashCode(of());
            result = 31 * result + Objects.hashCode(in());
            result = 31 * result + Objects.hashCode(using());
            result = 31 * result + Objects.hashCode(where());
            result = 31 * result + Objects.hashCode(includesAttributes());

            return result;
        }
    }

    public static class Cluster extends GraqlCompute.Configurable<GraqlCompute.Cluster> {

        final static long DEFAULT_K = 2L;

        Cluster(){
            super(Token.Compute.Method.CLUSTER, false);
        }

        protected GraqlCompute.Cluster self() {
            return this;
        }

        @CheckReturnValue
        public Token.Compute.Algorithm using() {
            if (algorithm == null) {
                return Token.Compute.Algorithm.CONNECTED_COMPONENT;
            } else {
                return algorithm;
            }
        }

        @Override
        public Set<Token.Compute.Algorithm> algorithmsAccepted() {
            return set(Token.Compute.Algorithm.CONNECTED_COMPONENT, Token.Compute.Algorithm.K_CORE);
        }

        @Override
        public Map<Token.Compute.Algorithm, Set<Token.Compute.Param>> argumentsAccepted() {
            return map(tuple(Token.Compute.Algorithm.K_CORE, set(Token.Compute.Param.K)),
                       tuple(Token.Compute.Algorithm.CONNECTED_COMPONENT, set(Token.Compute.Param.SIZE, Token.Compute.Param.CONTAINS)));
        }

        @Override
        public Map<Token.Compute.Algorithm, Map<Token.Compute.Param, Object>> argumentsDefault() {
            return map(tuple(Token.Compute.Algorithm.K_CORE, map(tuple(Token.Compute.Param.K, DEFAULT_K))));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraqlCompute.Cluster that = (GraqlCompute.Cluster) o;

            return (this.method().equals(that.method()) &&
                    this.in().equals(that.in()) &&
                    this.using().equals(that.using()) &&
                    this.where().equals(that.where()) &&
                    this.includesAttributes() == that.includesAttributes());
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(method());
            result = 31 * result + Objects.hashCode(in());
            result = 31 * result + Objects.hashCode(using());
            result = 31 * result + Objects.hashCode(where());
            result = 31 * result + Objects.hashCode(includesAttributes());

            return result;
        }
    }

    /**
     * Graql Compute argument objects to be passed into the query
     *
     * @param <T>
     */
    public static class Argument<T> implements Computable.Argument<T> {

        private Token.Compute.Param param;
        private T value;

        private Argument(Token.Compute.Param param, T value) {
            this.param = param;
            this.value = value;
        }

        public final Token.Compute.Param type() {
            return this.param;
        }

        public final T value() {
            return this.value;
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

        public static Argument<String> contains(String conceptId) {
            return new Argument<>(Token.Compute.Param.CONTAINS, conceptId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Argument<?> that = (Argument<?>) o;

            return (this.type().equals(that.type()) &&
                    this.value().equals(that.value()));
        }

        @Override
        public int hashCode() {
            int result = param.hashCode();
            result = 31 * result + value.hashCode();

            return result;
        }
    }

    /**
     * Argument inner class to provide access Compute Query arguments
     */
    public class Arguments implements Computable.Arguments {

        private LinkedHashMap<Token.Compute.Param, Argument> argumentsOrdered = new LinkedHashMap<>();
        private Map<Token.Compute.Param, Object> defaults = new HashMap<>();

        private final Map<Token.Compute.Param, Supplier<Optional<?>>> argumentsMap = argumentsMap();

        private Map<Token.Compute.Param, Supplier<Optional<?>>> argumentsMap() {
            Map<Token.Compute.Param, Supplier<Optional<?>>> arguments = new HashMap<>();
            arguments.put(Token.Compute.Param.MIN_K, this::minK);
            arguments.put(Token.Compute.Param.K, this::k);
            arguments.put(Token.Compute.Param.SIZE, this::size);
            arguments.put(Token.Compute.Param.CONTAINS, this::contains);

            return arguments;
        }

        private void setArgument(Argument<?> arg) {
            argumentsOrdered.remove(arg.type());
            argumentsOrdered.put(arg.type(), arg);
        }

        private void setDefaults(Map<Token.Compute.Param, Object> defaults) {
            this.defaults = defaults;
        }

        @CheckReturnValue
        Optional<?> getArgument(Token.Compute.Param param) {
            return argumentsMap.get(param).get();
        }

        @CheckReturnValue
        public Set<Token.Compute.Param> getParameters() {
            return argumentsOrdered.keySet();
        }

        @CheckReturnValue
        @Override
        public Optional<Long> minK() {
            Long minK = (Long) getArgumentValue(Token.Compute.Param.MIN_K);
            if (minK != null) {
                return Optional.of(minK);

            } else if (defaults.containsKey(Token.Compute.Param.MIN_K)){
                return Optional.of((Long) defaults.get(Token.Compute.Param.MIN_K));

            } else {
                return Optional.empty();
            }
        }

        @CheckReturnValue
        @Override
        public Optional<Long> k() {
            Long minK = (Long) getArgumentValue(Token.Compute.Param.K);
            if (minK != null) {
                return Optional.of(minK);

            } else if (defaults.containsKey(Token.Compute.Param.K)){
                return Optional.of((Long) defaults.get(Token.Compute.Param.K));

            } else {
                return Optional.empty();
            }
        }

        @CheckReturnValue
        @Override
        public Optional<Long> size() {
            return Optional.ofNullable((Long) getArgumentValue(Token.Compute.Param.SIZE));
        }

        @CheckReturnValue
        @Override
        public Optional<String> contains() {
            return Optional.ofNullable((String) getArgumentValue(Token.Compute.Param.CONTAINS));
        }

        private Object getArgumentValue(Token.Compute.Param param) {
            return argumentsOrdered.get(param) != null ? argumentsOrdered.get(param).value() : null;
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
