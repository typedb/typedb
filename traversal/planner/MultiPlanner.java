package com.vaticle.typedb.core.traversal.planner;

import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.procedure.PermutationProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class MultiPlanner implements Planner {

    private final List<ConnectedPlanner> planners;

    public MultiPlanner(List<ConnectedPlanner> planners) {
        this.planners = planners;
    }

    static MultiPlanner create(List<Structure> structures) {
        List<ConnectedPlanner> planners = new ArrayList<>(structures.size());
        structures.forEach(s -> planners.add(ConnectedPlanner.create(s)));
        // nb planning of disconnected planners should happen in parallel
        return new MultiPlanner(planners);
    }

    @Override
    public void tryOptimise(GraphManager graphMgr, boolean singleUse) {
        List<CompletableFuture<Void>> optimising = new ArrayList<>(planners.size());
        planners.forEach(planner -> optimising.add(CompletableFuture.runAsync(() -> planner.tryOptimise(graphMgr, singleUse))));
        CompletableFuture.allOf(optimising.toArray(new CompletableFuture[0])).join();

        // create procedure based on optimal ordering of all the planners
        GraphProcedure.Builder builder = new GraphProcedure.Builder();
        Comparator<Planner> comparator = Comparator.comparing(planner -> planner.isVertex() ? 1 : planner.asGraph().vertices().size()).reversed();
        planners.stream().sorted(comparator).forEach(planner -> GraphProcedure.Builder.create(builder, planner, builder.vertices().size()));
    }

    @Override
    public PermutationProcedure procedure() {
        return null;
    }

    @Override
    public boolean isOptimal() {
        return iterate(planners).allMatch(Planner::isOptimal);
    }
}
