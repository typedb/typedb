package ai.grakn.graql.internal.query.analytics;

import ai.grakn.Grakn;
import ai.grakn.GraknComputer;
import ai.grakn.GraknGraph;
import ai.grakn.factory.GraknSessionMock;

import java.util.Optional;

/**
 *
 */
public class DegreeQueryImplMock extends DegreeQueryImpl {
    final int numberOfWorkers;

    public DegreeQueryImplMock(Optional<GraknGraph> graph, int numberOfWorkers) {
        super(graph);
        this.numberOfWorkers = numberOfWorkers;
    }

    @Override
    protected GraknComputer getGraphComputer() {
        GraknSessionMock factory = new GraknSessionMock(keySpace, Grakn.DEFAULT_URI);
        return factory.getGraphComputer(numberOfWorkers);
    }
}
