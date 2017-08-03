/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graph.internal.computer.interceptor;

import ai.grakn.graph.internal.computer.GraknSparkMemory;
import ai.grakn.graph.internal.computer.GraknSparkVertexProgramInterceptor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.ProgramPhase;
import org.apache.tinkerpop.gremlin.process.computer.traversal.MemoryTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.finalization.ComputerFinalizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.NumberHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.function.ArrayListSupplier;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;

/**
 * @author Jason Liu
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GraknSparkStarBarrierInterceptor
        implements GraknSparkVertexProgramInterceptor<TraversalVertexProgram> {

    public GraknSparkStarBarrierInterceptor() {

    }

    @Override
    public JavaPairRDD<Object, VertexWritable> apply(final TraversalVertexProgram vertexProgram, final JavaPairRDD<Object, VertexWritable> inputRDD, final GraknSparkMemory memory) {
        vertexProgram.setup(memory);
        final Traversal.Admin<Vertex, Object> traversal = (Traversal.Admin) vertexProgram.getTraversal().getPure().clone();
        final GraphStep<Vertex, Vertex> graphStep = ((GraphStep) traversal.getStartStep());
        final Object[] graphStepIds = graphStep.getIds();    // any V(1,2,3)-style ids to filter on
        final ReducingBarrierStep endStep = (ReducingBarrierStep) traversal.getEndStep(); // needed for the final traverser generation
        traversal.removeStep(0);                                    // remove GraphStep
        traversal.removeStep(traversal.getSteps().size() - 1);      // remove ReducingBarrierStep
        traversal.setStrategies(traversal.clone().getStrategies().removeStrategies(ComputerVerificationStrategy.class, ComputerFinalizationStrategy.class)); // no longer a computer job, but parallel standard jobs
        traversal.applyStrategies();                                // compile
        boolean identityTraversal = traversal.getSteps().isEmpty(); // if the traversal is empty, just return the vertex (fast)
        ///////////////////////////////
        MemoryTraversalSideEffects.setMemorySideEffects(traversal, memory, ProgramPhase.EXECUTE); // any intermediate sideEffect steps are backed by SparkMemory
        memory.setInExecute(true);
        final JavaRDD<Traverser.Admin<Object>> nextRDD = inputRDD.values()
                .filter(vertexWritable -> ElementHelper.idExists(vertexWritable.get().id(), graphStepIds)) // ensure vertex ids are in V(x)
                .flatMap(vertexWritable -> {
                    if (identityTraversal) {                          // g.V.count()-style (identity)
                        return () -> IteratorUtils.of(traversal.getTraverserGenerator().generate(vertexWritable.get(), (Step) graphStep, 1l));
                    }else {                                          // add the vertex to head of the traversal
                        return () -> {                              // and iterate it for its results
                            final Traversal.Admin<Vertex, ?> clone = traversal.clone(); // need a unique clone for each vertex to isolate the computation
                            clone.getStartStep().addStart(clone.getTraverserGenerator().generate(vertexWritable.get(), graphStep, 1l));
                            return (Step) clone.getEndStep();
                        };
                    }
                });
        // USE SPARK DSL FOR THE RESPECTIVE END REDUCING BARRIER STEP OF THE TRAVERSAL
        final Object result;
        if (endStep instanceof CountGlobalStep){
            result = nextRDD.map(Traverser::bulk).fold(0l, (a, b) -> a + b);}
        else if (endStep instanceof SumGlobalStep){
            result = nextRDD
                    .map(traverser -> NumberHelper.mul(traverser.bulk(), (Number) traverser.get()))
                    .fold(0, NumberHelper::add);}
        else if (endStep instanceof MeanGlobalStep){
            result = nextRDD
                    .map(traverser -> new MeanGlobalStep.MeanNumber((Number) traverser.get(), traverser.bulk()))
                    .fold(new MeanGlobalStep.MeanNumber(), MeanGlobalStep.MeanNumber::add)
                    .getFinal();}
        else if (endStep instanceof MinGlobalStep){
            result = nextRDD
                    .map(traverser -> (Number) traverser.get())
                    .fold(Integer.MAX_VALUE, NumberHelper::min);}
        else if (endStep instanceof MaxGlobalStep){
            result = nextRDD
                    .map(traverser -> (Number) traverser.get())
                    .fold(Integer.MIN_VALUE, NumberHelper::max);}
        else if (endStep instanceof FoldStep) {
            final BinaryOperator biOperator = endStep.getBiOperator();
            result = nextRDD.map(traverser -> {
                if (endStep.getSeedSupplier() instanceof ArrayListSupplier) {
                    final List list = new ArrayList<>();
                    for (long i = 0; i < traverser.bulk(); i++) {
                        list.add(traverser.get());
                    }
                    return list;
                } else {
                    return traverser.get();
                }
            }).fold(endStep.getSeedSupplier().get(), biOperator::apply);
        } else if (endStep instanceof GroupStep) {
            final GroupStep.GroupBiOperator<Object, Object> biOperator = (GroupStep.GroupBiOperator) endStep.getBiOperator();
            result = ((GroupStep) endStep).generateFinalResult(nextRDD.
                    mapPartitions(partitions -> {
                        final GroupStep<Object, Object, Object> clone = (GroupStep) endStep.clone();
                        return () -> IteratorUtils.map(partitions, clone::projectTraverser);
                    }).fold(((GroupStep<Object, Object, Object>) endStep).getSeedSupplier().get(), biOperator::apply));
        } else if (endStep instanceof GroupCountStep) {
            final GroupCountStep.GroupCountBiOperator<Object> biOperator = GroupCountStep.GroupCountBiOperator.instance();
            result = nextRDD
                    .mapPartitions(partitions -> {
                        final GroupCountStep<Object, Object> clone = (GroupCountStep) endStep.clone();
                        return () -> IteratorUtils.map(partitions, clone::projectTraverser);
                    })
                    .fold(((GroupCountStep<Object, Object>) endStep).getSeedSupplier().get(), biOperator::apply);
        } else{
            throw new IllegalArgumentException("The end step is an unsupported barrier: " + endStep);}
        memory.setInExecute(false);
        ///////////////////////////////

        // generate the HALTED_TRAVERSERS for the memory
        final TraverserSet<Long> haltedTraversers = new TraverserSet<>();
        haltedTraversers.add(traversal.getTraverserGenerator().generate(result, endStep, 1l)); // all reducing barrier steps produce a result of bulk 1
        memory.set(TraversalVertexProgram.HALTED_TRAVERSERS, haltedTraversers);
        memory.incrIteration(); // any local star graph reduction takes a single iteration
        return inputRDD;
    }

    public static boolean isLegal(final Traversal.Admin<?, ?> traversal) {
        final Step<?, ?> startStep = traversal.getStartStep();
        final Step<?, ?> endStep = traversal.getEndStep();
        // right now this is not supported because of how the SparkStarBarrierInterceptor mutates the traversal prior to local evaluation
        if (traversal.getStrategies().toList().stream().filter(strategy -> strategy instanceof SubgraphStrategy).findAny().isPresent())
        {return false;}
        if (!startStep.getClass().equals(GraphStep.class) || ((GraphStep) startStep).returnsEdge())
        {return false;}
        if (!endStep.getClass().equals(CountGlobalStep.class) &&
                !endStep.getClass().equals(SumGlobalStep.class) &&
                !endStep.getClass().equals(MeanGlobalStep.class) &&
                !endStep.getClass().equals(MaxGlobalStep.class) &&
                !endStep.getClass().equals(MinGlobalStep.class) &&
                !endStep.getClass().equals(FoldStep.class) &&
                !endStep.getClass().equals(GroupStep.class) &&
                !endStep.getClass().equals(GroupCountStep.class))
            // TODO: tree()
        {return false;}
        if (TraversalHelper.getStepsOfAssignableClassRecursively(Scope.global, Barrier.class, traversal).size() != 1)
        { return false;}
        if (traversal.getTraverserRequirements().contains(TraverserRequirement.SACK))
        {return false;}
        return TraversalHelper.isLocalStarGraph(traversal);
    }
}

