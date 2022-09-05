package com.vaticle.typedb.core.traversal.planner;

import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.procedure.PermutationProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class MultiPlanner implements Planner {

    private final List<ConnectedPlanner> planners;
    private final AtomicBoolean isOptimising;
    private CompletableFuture<Void> optimisation;
    private GraphProcedure procedure;

    public MultiPlanner(List<ConnectedPlanner> planners) {
        this.planners = planners;
        this.isOptimising = new AtomicBoolean(false);
    }

    static MultiPlanner create(List<Structure> structures) {
        List<ConnectedPlanner> planners = new ArrayList<>(structures.size());
        structures.forEach(s -> planners.add(ConnectedPlanner.create(s)));
        return new MultiPlanner(planners);
    }

    @Override
    public void tryOptimise(GraphManager graphMgr, boolean singleUse) {
        if (isOptimising.compareAndSet(false, true)) mayOptimise(graphMgr, singleUse);
        optimisation.join();
    }

    private synchronized void mayOptimise(GraphManager graphMgr, boolean singleUse) {
        if (isOptimal()) return;
        List<CompletableFuture<Void>> futures = new ArrayList<>(planners.size());
        planners.forEach(planner -> futures.add(CompletableFuture.runAsync(() -> planner.tryOptimise(graphMgr, singleUse))));
        optimisation = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenRun(this::createProcedure);
    }

    private void createProcedure() {
        // create procedure based on optimal ordering of all the planners, heuristically estimated
        Comparator<Planner> comparator = Comparator.<Planner, Integer>comparing(planner ->
                planner.isVertex() ? 1 : planner.asGraph().vertices().size()
        ).reversed();
        procedure = GraphProcedure.create(planners.stream().sorted(comparator).collect(Collectors.toList()));
    }

    @Override
    public PermutationProcedure procedure() {
        return procedure;
    }

    @Override
    public boolean isOptimal() {
        return iterate(planners).allMatch(Planner::isOptimal);
    }
}
