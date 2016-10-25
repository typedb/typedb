package io.mindmaps.graql.internal.gremlin.fragment;

import io.mindmaps.graql.internal.gremlin.EquivalentFragmentSet;

import java.util.Optional;

abstract class AbstractFragment implements Fragment{

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
}
