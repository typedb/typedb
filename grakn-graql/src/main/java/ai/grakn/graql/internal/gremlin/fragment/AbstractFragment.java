package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;

import java.util.Optional;

abstract class AbstractFragment implements Fragment{

    // TODO: Find a better way to represent these values (either abstractly, or better estimates)
    static final long NUM_INSTANCES_PER_TYPE = 1_000;
    static final long NUM_INSTANCES_PER_SCOPE = 1_000;
    static final long NUM_RELATION_PER_CASTING = 1_000;
    static final long NUM_SHORTCUT_EDGES_PER_INSTANCE = 10;
    static final long NUM_SUBTYPES_PER_TYPE = 10;
    static final long NUM_CASTINGS_PER_INSTANCE = 10;
    static final long NUM_SCOPES_PER_INSTANCE = 10;
    static final long NUM_TYPES_PER_ROLE = 10;
    static final long NUM_ROLES_PER_TYPE = 10;
    static final long NUM_ROLES_PER_RELATION = 2;

    private final String start;
    private final Optional<String> end;
    private EquivalentFragmentSet equivalentFragmentSet;

    AbstractFragment(String start) {
        this.start = start;
        this.end = Optional.empty();
    }

    AbstractFragment(String start, String end) {
        this.start = start;
        this.end = Optional.of(end);
    }

    @Override
    public final EquivalentFragmentSet getEquivalentFragmentSet() {
        return equivalentFragmentSet;
    }

    @Override
    public final void setEquivalentFragmentSet(EquivalentFragmentSet equivalentFragmentSet) {
        this.equivalentFragmentSet = equivalentFragmentSet;
    }

    @Override
    public final String getStart() {
        return start;
    }

    @Override
    public final Optional<String> getEnd() {
        return end;
    }

    @Override
    public final int compareTo(@SuppressWarnings("NullableProblems") Fragment other) {
        // Don't want to use Jetbrain's @NotNull annotation
        if (this == other) return 0;
        return getPriority().compareTo(other.getPriority());
    }

    @Override
    public String toString() {
        return "$" + start + getName() + end.map(e -> "$" + e).orElse("");
    }
}
