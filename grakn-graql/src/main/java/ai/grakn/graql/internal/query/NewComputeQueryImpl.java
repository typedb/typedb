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
import static ai.grakn.util.GraqlSyntax.Compute.Condition.OF;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.TO;
import static ai.grakn.util.GraqlSyntax.Compute.Condition.USING;
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
    private Optional<ConceptId> fromID = Optional.empty();
    private Optional<ConceptId> toID = Optional.empty();
    private Optional<Set<Label>> ofTypes = Optional.empty();
    private Optional<Set<Label>> inTypes = Optional.empty();
    private Optional<String> algorithm = Optional.empty();
    private Optional<Long> minK = Optional.empty();
    private boolean includeAttribute;

    private final static long DEFAULT_MIN_K = 2L;
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
    public final String method() {
        return method;
    }

    @Override
    public final NewComputeQuery from(ConceptId fromID) {
        this.fromID = Optional.ofNullable(fromID);
        return this;
    }

    @Override
    public final Optional<ConceptId> from() {
        return fromID;
    }

    @Override
    public final NewComputeQuery to(ConceptId toID) {
        this.toID = Optional.ofNullable(toID);
        return this;
    }

    @Override
    public final Optional<ConceptId> to() {
        return toID;
    }

    @Override
    public final NewComputeQuery of(String... types) {
        return of(Arrays.stream(types).map(Label::of).collect(toImmutableSet()));
    }

    @Override
    public final NewComputeQuery of(Collection<Label> types) {
        this.ofTypes = Optional.of(ImmutableSet.copyOf(types));
        return this;
    }

    @Override
    public final Optional<Set<Label>> of() {
        return ofTypes;
    }

    @Override
    public final NewComputeQuery in(String... types) {
        return in(Arrays.stream(types).map(Label::of).collect(toImmutableSet()));
    }

    @Override
    public final NewComputeQuery in(Collection<Label> types) {
        this.inTypes = Optional.of(ImmutableSet.copyOf(types));
        return this;
    }

    @Override
    public final Optional<Set<Label>> in() {
        return inTypes;
    }

    @Override
    public final NewComputeQuery using(String algorithm) {
        this.algorithm = Optional.of(algorithm);
        return this;
    }

    @Override
    public final Optional<String> using() {
        return algorithm;
    }

    @Override
    public final NewComputeQuery minK(long minK) {
        this.minK = Optional.of(minK);
        return this;
    }

    @Override
    public final Optional<Long> minK() {
        return minK;
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

    private Optional<GraqlQueryException> collectExceptions() {
        //TODO

        return Optional.empty();
    }

    @Override
    public final String toString() {
        StringBuilder query = new StringBuilder();

        query.append(COMPUTE + SPACE + methodSyntax());
        if (!conditionsSyntax().isEmpty()) query.append(SPACE + conditionsSyntax());
        query.append(SEMICOLON);

        return query.toString();
    }

    private String methodSyntax() {
        return method;
    }

    private String conditionsSyntax() {
        List<String> conditionsList = new ArrayList<>();

        if (fromID.isPresent()) conditionsList.add(FROM + SPACE + QUOTE + fromID.get() + QUOTE);
        if (toID.isPresent()) conditionsList.add(TO + SPACE + QUOTE + toID.get() + QUOTE);
        if (ofTypes.isPresent()) conditionsList.add(ofTypesSyntax());
        if (inTypes.isPresent()) conditionsList.add(inTypesSyntax());
        if (algorithm.isPresent()) conditionsList.add(algorithmSyntax());

        return conditionsList.stream().collect(joining(COMMA_SPACE));
    }

    private String ofTypesSyntax() {
        if (ofTypes.isPresent()) return OF + SPACE + typesSyntax(ofTypes);

        return "";
    }

    private String inTypesSyntax() {
        if (inTypes.isPresent()) return IN + SPACE + typesSyntax(inTypes);

        return "";
    }

    private String typesSyntax(Optional<Set<Label>> types) {
        StringBuilder inTypesString = new StringBuilder();

        if (types.isPresent() && !types.get().isEmpty()) {
            if (types.get().size() == 1) inTypesString.append(StringConverter.typeLabelToString(types.get().iterator().next()));
            else {
                inTypesString.append(SQUARE_OPEN);
                inTypesString.append(inTypes.get().stream().map(StringConverter::typeLabelToString).collect(joining(COMMA_SPACE)));
                inTypesString.append(SQUARE_CLOSE);
            }
        }

        return inTypesString.toString();
    }

    private String algorithmSyntax() {
        if (algorithm.isPresent()) return USING + SPACE + algorithm.get();

        return "";
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
                this.minK().equals(that.minK()) &&
                this.isAttributeIncluded() == that.isAttributeIncluded();
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
        result = 31 * result + minK.hashCode();
        result = 31 * result + Boolean.hashCode(includeAttribute);
        return result;
    }
}
