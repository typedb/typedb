package com.vaticle.typedb.core.traversal.planner;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.traversal.procedure.GraphProcedure;
import com.vaticle.typedb.core.traversal.procedure.PermutationProcedure;
import com.vaticle.typedb.core.traversal.structure.Structure;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;

public class MultiPlanner implements Planner {

    private final List<ConnectedPlanner> planners;
    private final Semaphore optimisationLock;
    private GraphProcedure procedure;

    public MultiPlanner(List<ConnectedPlanner> planners) {
        this.planners = planners;
        this.optimisationLock = new Semaphore(1);
    }

    static MultiPlanner create(List<Structure> structures) {
        List<ConnectedPlanner> planners = new ArrayList<>(structures.size());
        structures.forEach(s -> planners.add(ConnectedPlanner.create(s)));
        return new MultiPlanner(planners);
    }

    @Override
    public void tryOptimise(GraphManager graphMgr, boolean singleUse) {
        if (optimisationLock.tryAcquire()) {
            mayOptimise(graphMgr, singleUse);
            optimisationLock.release();
        } else {
            try {
                // await current optimisation
                optimisationLock.acquire();
                optimisationLock.release();
            } catch (InterruptedException e) {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }
    }

    private void mayOptimise(GraphManager graphMgr, boolean singleUse) {
        if (isOptimal()) {
            if (procedure == null) createProcedure();
            return;
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>(planners.size());
        planners.forEach(planner -> futures.add(CompletableFuture.runAsync(() -> planner.tryOptimise(graphMgr, singleUse))));
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        createProcedure();
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
