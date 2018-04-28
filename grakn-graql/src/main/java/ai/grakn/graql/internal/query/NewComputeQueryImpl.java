package ai.grakn.graql.internal.query;

import ai.grakn.ComputeJob;
import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.ComputeAnswer;
import ai.grakn.graql.NewComputeQuery;
import ai.grakn.graql.internal.util.StringConverter;
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
import static ai.grakn.util.GraqlSyntax.Compute.Condition.FROM;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.IN;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.TO;
import static ai.grakn.util.GraqlSyntax.QUOTE;
import static ai.grakn.util.GraqlSyntax.SEMICOLON;
import static ai.grakn.util.GraqlSyntax.SPACE;
import static ai.grakn.util.GraqlSyntax.SQUARE_CLOSE;
import static ai.grakn.util.GraqlSyntax.SQUARE_OPEN;
import static java.util.stream.Collectors.joining;

public class NewComputeQueryImpl extends AbstractQuery<ComputeAnswer, ComputeAnswer> implements NewComputeQuery {


    private Optional<GraknTx> tx;
    private Set<ComputeJob<ComputeAnswer>> runningJobs = ConcurrentHashMap.newKeySet();

    private String method;
    private Optional<ConceptId> from = Optional.empty();
    private Optional<ConceptId> to = Optional.empty();
    protected ImmutableSet<Label> inTypes = ImmutableSet.of();
    private boolean includeAttribute;


    private static final boolean DEFAULT_INCLUDE_ATTRIBUTE = false;

    NewComputeQueryImpl(String method, Optional<GraknTx> tx) {
        this(method, tx, DEFAULT_INCLUDE_ATTRIBUTE);
    }

    NewComputeQueryImpl(String method, Optional<GraknTx> tx, boolean includeAttribute) {
        this.method = method;
        this.tx = tx;
        this.includeAttribute = includeAttribute;
    }

    @Override
    public final Stream<ComputeAnswer> stream() {
        return Stream.of(execute());
    }

    @Override
    public final ComputeAnswer execute() {
        Optional<GraqlQueryException> exception = collectExceptions();
        if (exception.isPresent()) throw exception.get();

        ComputeJob<ComputeAnswer> job = null;//TODO: executor().run(this);

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
    public final NewComputeQuery from(ConceptId fromID) {
        this.from = Optional.ofNullable(fromID);
        return this;
    }

    @Override
    public final Optional<ConceptId> from() {
        return from;
    }

    @Override
    public final NewComputeQuery to(ConceptId toID) {
        this.to = Optional.ofNullable(toID);
        return this;
    }

    @Override
    public final Optional<ConceptId> to() {
        return to;
    }

    @Override
    public final NewComputeQuery in(String... inTypes) {
        return in(Arrays.stream(inTypes).map(Label::of).collect(toImmutableSet()));
    }

    @Override
    public final NewComputeQuery in(Collection<? extends Label> inTypes) {
        this.inTypes = ImmutableSet.copyOf(inTypes);
        return this;
    }

    @Override
    public final ImmutableSet<Label> inTypes() {
        return inTypes;
    }

    @Override
    public final NewComputeQueryImpl includeAttribute() {
        this.includeAttribute = true;
        return this;
    }

    @Override
    public final boolean isAttributeIncluded() {
        return includeAttribute;
    }

    @Override
    public final Boolean inferring() {
        return false;
    }

    @Override
    public final boolean isValid() {
        return !collectExceptions().isPresent();
    }

    private final Optional<GraqlQueryException> collectExceptions() {
        //TODO

        return Optional.empty();
    }

    private final String methodString() {
        return method;
    }

    private final String conditionsString() {
        List<String> conditionsList = new ArrayList<>();

        if (from.isPresent()) conditionsList.add(FROM + SPACE + QUOTE + from.get() + QUOTE);
        if (to.isPresent()) conditionsList.add(TO + SPACE + QUOTE + to.get() + QUOTE);
        if (!inTypesString().isEmpty()) conditionsList.add(inTypesString());

        return conditionsList.stream().collect(joining(COMMA_SPACE));
    }

    final String inTypesString() {
        if (!inTypes.isEmpty()) return IN + SPACE + typesString(inTypes);

        return "";
    }

    final String typesString(ImmutableSet<Label> types) {
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

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(COMPUTE + SPACE + methodString());

        if (!conditionsString().isEmpty()) {
            query.append(SPACE + conditionsString());
        }

        query.append(SEMICOLON);

        return query.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NewComputeQuery that = (NewComputeQuery) o;

        return tx.equals(that.tx()) && includeAttribute == that.isAttributeIncluded() && inTypes.equals(that.inTypes());
    }

    @Override
    public int hashCode() {
        int result = tx.hashCode();
        result = 31 * result + Boolean.hashCode(includeAttribute);
        result = 31 * result + inTypes.hashCode();
        return result;
    }
}
