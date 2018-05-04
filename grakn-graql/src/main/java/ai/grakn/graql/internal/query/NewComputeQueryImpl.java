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

package ai.grakn.graql.internal.query;

import ai.grakn.ComputeJob;
import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.ComputeAnswer;
import ai.grakn.graql.NewComputeQuery;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static ai.grakn.util.CommonUtil.toImmutableSet;
import static ai.grakn.util.GraqlSyntax.COMMA_SPACE;
import static ai.grakn.util.GraqlSyntax.COMPUTE;
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm;
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.CONNECTED_COMPONENT;
import static ai.grakn.util.GraqlSyntax.Compute.Algorithm.K_CORE;
import static ai.grakn.util.GraqlSyntax.Compute.Argument;
import static ai.grakn.util.GraqlSyntax.Compute.Argument.DEFAULT_K;
import static ai.grakn.util.GraqlSyntax.Compute.Argument.DEFAULT_MEMBERS;
import static ai.grakn.util.GraqlSyntax.Compute.Argument.DEFAULT_MIN_K;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.FROM;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.IN;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.OF;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.TO;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.USING;
import static ai.grakn.util.GraqlSyntax.Compute.Method;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CENTRALITY;
import static ai.grakn.util.GraqlSyntax.Compute.Method.CLUSTER;
import static ai.grakn.util.GraqlSyntax.QUOTE;
import static ai.grakn.util.GraqlSyntax.SEMICOLON;
import static ai.grakn.util.GraqlSyntax.SPACE;
import static ai.grakn.util.GraqlSyntax.SQUARE_CLOSE;
import static ai.grakn.util.GraqlSyntax.SQUARE_OPEN;
import static java.util.stream.Collectors.joining;

public class NewComputeQueryImpl extends AbstractQuery<ComputeAnswer, ComputeAnswer> implements NewComputeQuery {

    private Optional<GraknTx> tx;
    private Set<ComputeJob<ComputeAnswer>> runningJobs = ConcurrentHashMap.newKeySet();

    private Method method;
    private ConceptId fromID = null;
    private ConceptId toID = null;
    private Set<Label> ofTypes = null;
    private Set<Label> inTypes = null;
    private Algorithm algorithm = null;
    private ArgumentsImpl arguments = null;
    private boolean includeAttributes = false;

    public NewComputeQueryImpl(Optional<GraknTx> tx, Method method) {
        this(tx, method, false);
    }

    public NewComputeQueryImpl(Optional<GraknTx> tx, Method method, boolean includeAttributes) {
        this.method = method;
        this.tx = tx;
        this.includeAttributes = includeAttributes;
    }

    @Override
    public final Stream<ComputeAnswer> stream() {
        return Stream.of(execute());
    }

    @Override
    public final ComputeAnswer execute() {
        Optional<GraqlQueryException> exception = getException();
        if (exception.isPresent()) throw exception.get();

        ComputeJob<ComputeAnswer> job = executor().run(this);

        runningJobs.add(job);

        try {
            return job.get();
        } finally {
            runningJobs.remove(job);
        }
    }

    @Override
    public final void kill() {
        runningJobs.forEach(ComputeJob::kill);
    }

    @Override
    public final NewComputeQuery withTx(GraknTx tx) {
        this.tx = Optional.of(tx);
        return this;
    }

    @Override
    public final Optional<GraknTx> tx() {
        return tx;
    }

    @Override
    public final Method method() {
        return method;
    }

    @Override
    public final NewComputeQuery from(ConceptId fromID) {
        this.fromID = fromID;
        return this;
    }

    @Override
    public final Optional<ConceptId> from() {
        return Optional.ofNullable(fromID);
    }

    @Override
    public final NewComputeQuery to(ConceptId toID) {
        this.toID = toID;
        return this;
    }

    @Override
    public final Optional<ConceptId> to() {
        return Optional.ofNullable(toID);
    }

    @Override
    public final NewComputeQuery of(String type, String... types) {
        ArrayList<String> typeList = new ArrayList<>(types.length + 1);
        typeList.add(type);
        typeList.addAll(Arrays.asList(types));

        return of(typeList.stream().map(Label::of).collect(toImmutableSet()));
    }

    @Override
    public final NewComputeQuery of(Collection<Label> types) {
        this.ofTypes = ImmutableSet.copyOf(types);

        return this;
    }

    @Override
    public final Optional<Set<Label>> of() {
        return Optional.ofNullable(ofTypes);
    }

    @Override
    public final NewComputeQuery in(String type, String... types) {
        ArrayList<String> typeList = new ArrayList<>(types.length + 1);
        typeList.add(type);
        typeList.addAll(Arrays.asList(types));

        return in(typeList.stream().map(Label::of).collect(toImmutableSet()));
    }

    @Override
    public final NewComputeQuery in(Collection<Label> types) {
        this.inTypes = ImmutableSet.copyOf(types);
        return this;
    }

    @Override
    public final Optional<Set<Label>> in() {
        if (this.inTypes == null) return Optional.of(ImmutableSet.of());
        return Optional.of(this.inTypes);
    }

    @Override
    public final NewComputeQuery using(Algorithm algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    @Override
    public final Optional<Algorithm> using() {
        return Optional.ofNullable(algorithm);
    }

    @Override
    public final NewComputeQuery where(Argument arg, Argument... args) {
        if (this.arguments == null) this.arguments = new ArgumentsImpl();

        ArrayList<Argument> argList = new ArrayList(args.length + 1);
        argList.add(arg);
        argList.addAll(Arrays.asList(args));

        for (Argument a : argList) {
            switch (a.type()) {
                case MIN_K:
                    this.arguments.minK((Long) a.get());
                    break;
                case K:
                    this.arguments.k((Long) a.get());
                    break;
                case SIZE:
                    this.arguments.size((Long) a.get());
                    break;
                case MEMBERS:
                    this.arguments.members((Boolean) a.get());
                    break;
                case CONTAINS:
                    this.arguments.contains((ConceptId) a.get());
                    break;
            }
        }

        return this;
    }

    @Override
    public final Optional<Arguments> where() {
        if ((method.equals(CENTRALITY) || method.equals(CLUSTER)) && arguments == null) arguments = new ArgumentsImpl();
        return Optional.of(this.arguments);
    }

    @Override
    public final NewComputeQueryImpl includeAttributes(boolean include) {
        this.includeAttributes = include;
        return this;
    }

    @Override
    public final boolean includesAttributes() {
        return includeAttributes;
    }

    @Override
    public final Boolean inferring() {
        return false;
    }

    @Override
    public final boolean isValid() {
        return !getException().isPresent();
    }

    @Override
    public Optional<GraqlQueryException> getException() {
        //TODO

        if (method.equals(Method.PATH) && (!from().isPresent() || !to().isPresent())) {
            return Optional.of(GraqlQueryException.invalidComputePathMissingCondition());
        }

        return Optional.empty();
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(COMPUTE + SPACE + method);
        if (!conditionsSyntax().isEmpty()) query.append(SPACE + conditionsSyntax());
        query.append(SEMICOLON);

        return query.toString();
    }

    private String conditionsSyntax() {
        List<String> conditionsList = new ArrayList<>();

        if (fromID != null) conditionsList.add(FROM + SPACE + QUOTE + fromID + QUOTE);
        if (toID != null) conditionsList.add(TO + SPACE + QUOTE + toID + QUOTE);
        if (ofTypes != null) conditionsList.add(ofSyntax());
        if (inTypes != null) conditionsList.add(inSyntax());
        if (algorithm != null) conditionsList.add(algorithmSyntax());
        if (arguments != null) conditionsList.add(argumentsSyntax());

        return conditionsList.stream().collect(joining(COMMA_SPACE));
    }

    private String ofSyntax() {
        if (ofTypes != null) return OF + SPACE + typesSyntax(ofTypes);

        return "";
    }

    private String inSyntax() {
        if (inTypes != null) return IN + SPACE + typesSyntax(inTypes);

        return "";
    }

    private String typesSyntax(Set<Label> types) {
        StringBuilder inTypesString = new StringBuilder();

        if (!types.isEmpty()) {
            if (types.size() == 1) inTypesString.append(StringConverter.typeLabelToString(types.iterator().next()));
            else {
                inTypesString.append(SQUARE_OPEN);
                inTypesString.append(inTypes.stream().map(StringConverter::typeLabelToString).collect(joining(COMMA_SPACE)));
                inTypesString.append(SQUARE_CLOSE);
            }
        }

        return inTypesString.toString();
    }

    private String algorithmSyntax() {
        if (algorithm != null) return USING + SPACE + algorithm;

        return "";
    }

    private String argumentsSyntax() {
        return ""; // TODO: write argument syntax algorithm
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NewComputeQuery that = (NewComputeQuery) o;

        return (this.tx().equals(that.tx()) &&
                this.method().equals(that.method()) &&
                this.from().equals(that.from()) &&
                this.to().equals(that.to()) &&
                this.of().equals(that.of()) &&
                this.in().equals(that.in())) &&
                this.using().equals(that.using()) &&
                this.where().equals(that.where()) &&
                this.includesAttributes() == that.includesAttributes();
    }

    @Override
    public int hashCode() {
        int result = tx.hashCode();
        result = 31 * result + method.hashCode();
        result = 31 * result + fromID.hashCode();
        result = 31 * result + toID.hashCode();
        result = 31 * result + ofTypes.hashCode();
        result = 31 * result + inTypes.hashCode();
        result = 31 * result + algorithm.hashCode();
        result = 31 * result + arguments.hashCode();
        result = 31 * result + Boolean.hashCode(includeAttributes);

        return result;
    }


    public class ArgumentsImpl implements Arguments {
        private Long minK = null;
        private Long k = null;
        private Long size = null;
        private Boolean members = null;
        private ConceptId contains = null;

        @Override
        public Optional<Long> minK() {
            if (method.equals(CENTRALITY) && algorithm.equals(K_CORE) && minK == null) {
                return Optional.of(DEFAULT_MIN_K);
            }

            return Optional.ofNullable(minK);
        }

        private void minK(long minK) {
            this.minK = minK;
        }

        @Override
        public Optional<Long> k() {
            if (method.equals(CLUSTER) && algorithm.equals(K_CORE) && k == null) {
                return Optional.of(DEFAULT_K);
            }

            return Optional.ofNullable(k);
        }

        private void k(long k) {
            this.k = k;
        }

        @Override
        public Optional<Long> size() {
            return Optional.ofNullable(size);
        }

        private void size(long k) {
            this.size = size;
        }

        @Override
        public Optional<Boolean> members() {
            if (method.equals(CLUSTER) && algorithm.equals(CONNECTED_COMPONENT) && members == null) {
                return Optional.of(DEFAULT_MEMBERS);
            }

            return Optional.ofNullable(members);
        }

        private void members(boolean members) {
            this.members = members;
        }

        @Override
        public Optional<ConceptId> contains() {
            return Optional.ofNullable(contains);
        }

        private void contains(ConceptId conceptId) {
            this.contains = conceptId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ArgumentsImpl that = (ArgumentsImpl) o;

            return (this.minK().equals(that.minK()) &&
                    this.k().equals(that.k()) &&
                    this.size().equals(that.size()) &&
                    this.members().equals(that.members()) &&
                    this.contains().equals(that.contains()));
        }

        @Override
        public int hashCode() {
            int result = tx.hashCode();
            result = 31 * result + minK.hashCode();
            result = 31 * result + k.hashCode();
            result = 31 * result + size.hashCode();
            result = 31 * result + members.hashCode();
            result = 31 * result + contains.hashCode();

            return result;
        }
    }
}
