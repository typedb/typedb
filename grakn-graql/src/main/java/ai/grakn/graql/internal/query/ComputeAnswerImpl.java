package ai.grakn.graql.internal.query;

import ai.grakn.concept.Concept;
import ai.grakn.graql.ComputeAnswer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;

public class ComputeAnswerImpl implements ComputeAnswer {

    private Optional<List<List<Concept>>> paths;

    public ComputeAnswerImpl() {}

    public Optional<List<List<Concept>>> paths() {
        return paths;
    }

    public ComputeAnswer paths(List<List<Concept>> paths) {
        this.paths = Optional.of(ImmutableList.copyOf(paths));
        return this;
    }
}
