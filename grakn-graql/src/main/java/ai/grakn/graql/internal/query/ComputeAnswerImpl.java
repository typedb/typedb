package ai.grakn.graql.internal.query;

import ai.grakn.concept.Concept;
import ai.grakn.graql.ComputeAnswer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;

public class ComputeAnswerImpl implements ComputeAnswer {

    private Long count = null;
    private List<List<Concept>> paths = null;

    public ComputeAnswerImpl() {}

    public Optional<Long> count() {
        return Optional.ofNullable(count);
    }

    public ComputeAnswer count(Long count) {
        this.count = count;
        return this;
    }

    public Optional<List<List<Concept>>> paths() {
        return Optional.ofNullable(paths);
    }

    public ComputeAnswer paths(List<List<Concept>> paths) {
        this.paths = ImmutableList.copyOf(paths);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;

        ComputeAnswer that = (ComputeAnswer) o;

        return (this.paths().equals(that.paths()) &&
                this.count().equals(that.count()));
    }

    @Override
    public int hashCode() {
        int result = count.hashCode();
        result = 31 * result + paths.hashCode();

        return result;
    }
}
