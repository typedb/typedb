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

package ai.grakn.graph.internal.computer;

import com.google.common.base.Optional;
import org.apache.commons.configuration.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPools;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.spark.process.computer.CombineIterator;
import org.apache.tinkerpop.gremlin.spark.process.computer.MapIterator;
import org.apache.tinkerpop.gremlin.spark.process.computer.ReduceIterator;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkMessenger;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.MessagePayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.Payload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewIncomingPayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewOutgoingPayload;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewPayload;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * <p>
 * This is a modified version of Spark Executor.
 * We change its behaviour so it can work with our own graph computer.
 * </p>
 *
 * @author Jason Liu
 * @author Marko A. Rodriguez
 */
public class GraknSparkExecutor {

    private static final String[] EMPTY_ARRAY = new String[0];

    private GraknSparkExecutor() {
    }

    ////////////////////
    // VERTEX PROGRAM //
    ////////////////////

    public static <M> JavaPairRDD<Object, ViewIncomingPayload<M>> executeVertexProgramIteration(
            final JavaPairRDD<Object, VertexWritable> graphRDD,
            final JavaPairRDD<Object, ViewIncomingPayload<M>> viewIncomingRDD,
            final GraknSparkMemory memory,
            final Configuration apacheConfiguration) {

        // the graphRDD and the viewRDD must have the same partitioner
        if (null != viewIncomingRDD) assert graphRDD.partitioner().get().equals(viewIncomingRDD.partitioner().get());
        final JavaPairRDD<Object, ViewOutgoingPayload<M>> viewOutgoingRDD = (((null == viewIncomingRDD) ?
                graphRDD.mapValues(vertexWritable -> new Tuple2<>(vertexWritable, Optional.<ViewIncomingPayload<M>>absent())) : // first iteration will not have any views or messages
                graphRDD.leftOuterJoin(viewIncomingRDD))                                                   // every other iteration may have views and messages
                // for each partition of vertices emit a view and their outgoing messages
                .mapPartitionsToPair(partitionIterator -> {
                    HadoopPools.initialize(apacheConfiguration);
                    final VertexProgram<M> workerVertexProgram = VertexProgram.<VertexProgram<M>>createVertexProgram(HadoopGraph.open(apacheConfiguration), apacheConfiguration); // each partition(Spark)/worker(TP3) has a local copy of the vertex program (a worker's task)
                    final Set<String> elementComputeKeys = workerVertexProgram.getElementComputeKeys(); // the compute keys as a set
                    final String[] elementComputeKeysArray = elementComputeKeys.size() == 0 ? EMPTY_ARRAY : elementComputeKeys.toArray(new String[elementComputeKeys.size()]); // the compute keys as an array
                    final SparkMessenger<M> messenger = new SparkMessenger<>();
                    workerVertexProgram.workerIterationStart(memory.asImmutable()); // start the worker
                    return () -> IteratorUtils.map(partitionIterator, vertexViewIncoming -> {
                        final StarGraph.StarVertex vertex = vertexViewIncoming._2()._1().get(); // get the vertex from the vertex writable
                        synchronized (vertex) {
                            // drop any computed properties that are cached in memory
                            if (elementComputeKeysArray.length > 0) {
                                vertex.dropVertexProperties(elementComputeKeysArray);
                            }
                            final boolean hasViewAndMessages = vertexViewIncoming._2()._2().isPresent(); // if this is the first iteration, then there are no views or messages
                            final List<DetachedVertexProperty<Object>> previousView = hasViewAndMessages ? vertexViewIncoming._2()._2().get().getView() : Collections.emptyList();
                            final List<M> incomingMessages = hasViewAndMessages ? vertexViewIncoming._2()._2().get().getIncomingMessages() : Collections.emptyList();
                            previousView.forEach(property -> property.attach(Attachable.Method.create(vertex)));  // attach the view to the vertex
                            // previousView.clear(); // no longer needed so kill it from memory
                            ///
                            messenger.setVertexAndIncomingMessages(vertex, incomingMessages); // set the messenger with the incoming messages
                            workerVertexProgram.execute(ComputerGraph.vertexProgram(vertex, workerVertexProgram), messenger, memory); // execute the vertex program on this vertex for this iteration
                            // incomingMessages.clear(); // no longer needed so kill it from memory
                            ///
                            final List<DetachedVertexProperty<Object>> nextView = elementComputeKeysArray.length == 0 ?  // not all vertex programs have compute keys
                                    Collections.emptyList() :
                                    IteratorUtils.list(IteratorUtils.map(vertex.properties(elementComputeKeysArray), property -> DetachedFactory.detach(property, true)));
                            final List<Tuple2<Object, M>> outgoingMessages = messenger.getOutgoingMessages(); // get the outgoing messages

                            // if no more vertices in the partition, end the worker's iteration
                            if (!partitionIterator.hasNext()) {
                                workerVertexProgram.workerIterationEnd(memory.asImmutable());
                            }
                            return new Tuple2<>(vertex.id(), new ViewOutgoingPayload<>(nextView, outgoingMessages));
                        }
                    });
                }, true)); // true means that the partition is preserved
        // the graphRDD and the viewRDD must have the same partitioner
        assert graphRDD.partitioner().get().equals(viewOutgoingRDD.partitioner().get());
        // "message pass" by reducing on the vertex object id of the view and message payloads
        final MessageCombiner<M> messageCombiner = VertexProgram.<VertexProgram<M>>createVertexProgram(HadoopGraph.open(apacheConfiguration), apacheConfiguration).getMessageCombiner().orElse(null);
        final JavaPairRDD<Object, ViewIncomingPayload<M>> newViewIncomingRDD = viewOutgoingRDD
                .flatMapToPair(tuple -> () -> IteratorUtils.<Tuple2<Object, Payload>>concat(
                        IteratorUtils.of(new Tuple2<>(tuple._1(), tuple._2().getView())),      // emit the view payload
                        IteratorUtils.map(tuple._2().getOutgoingMessages().iterator(), message -> new Tuple2<>(message._1(), new MessagePayload<>(message._2())))))  // emit the outgoing message payloads one by one
                .reduceByKey(graphRDD.partitioner().get(), (a, b) -> {      // reduce the view and outgoing messages into a single payload object representing the new view and incoming messages for a vertex
                    if (a instanceof ViewIncomingPayload) {
                        ((ViewIncomingPayload<M>) a).mergePayload(b, messageCombiner);
                        return a;
                    } else if (b instanceof ViewIncomingPayload) {
                        ((ViewIncomingPayload<M>) b).mergePayload(a, messageCombiner);
                        return b;
                    } else {
                        final ViewIncomingPayload<M> c = new ViewIncomingPayload<>(messageCombiner);
                        c.mergePayload(a, messageCombiner);
                        c.mergePayload(b, messageCombiner);
                        return c;
                    }
                })
                .filter(payload -> !(payload._2() instanceof MessagePayload)) // this happens if there is a message to a vertex that does not exist
                .filter(payload -> !((payload._2() instanceof ViewIncomingPayload) && !((ViewIncomingPayload<M>) payload._2()).hasView())) // this happens if there are many messages to a vertex that does not exist
                .mapValues(payload -> payload instanceof ViewIncomingPayload ?
                        (ViewIncomingPayload<M>) payload :                    // this happens if there is a vertex with incoming messages
                        new ViewIncomingPayload<>((ViewPayload) payload));    // this happens if there is a vertex with no incoming messages
        // the graphRDD and the viewRDD must have the same partitioner
        assert graphRDD.partitioner().get().equals(newViewIncomingRDD.partitioner().get());
        newViewIncomingRDD
                .foreachPartition(partitionIterator -> {
                    HadoopPools.initialize(apacheConfiguration);
                }); // need to complete a task so its BSP and the memory for this iteration is updated
        return newViewIncomingRDD;
    }

    public static <M> JavaPairRDD<Object, VertexWritable> prepareFinalGraphRDD(final JavaPairRDD<Object, VertexWritable> graphRDD, final JavaPairRDD<Object, ViewIncomingPayload<M>> viewIncomingRDD, final String[] elementComputeKeys) {
        // the graphRDD and the viewRDD must have the same partitioner
        assert (graphRDD.partitioner().get().equals(viewIncomingRDD.partitioner().get()));
        // attach the final computed view to the cached graph
        return graphRDD.leftOuterJoin(viewIncomingRDD)
                .mapValues(tuple -> {
                    final StarGraph.StarVertex vertex = tuple._1().get();
                    vertex.dropVertexProperties(elementComputeKeys);
                    final List<DetachedVertexProperty<Object>> view = tuple._2().isPresent() ? tuple._2().get().getView() : Collections.emptyList();
                    view.forEach(property -> property.attach(Attachable.Method.create(vertex)));
                    // view.clear(); // no longer needed so kill it from memory
                    return tuple._1();
                });
    }

    /////////////////
    // MAP REDUCE //
    ////////////////

    public static <K, V> JavaPairRDD<K, V> executeMap(final JavaPairRDD<Object, VertexWritable> graphRDD, final MapReduce<K, V, ?, ?, ?> mapReduce, final Configuration apacheConfiguration) {
        JavaPairRDD<K, V> mapRDD = graphRDD.mapPartitionsToPair(partitionIterator -> {
            HadoopPools.initialize(apacheConfiguration);
            return () -> new MapIterator<>(MapReduce.<MapReduce<K, V, ?, ?, ?>>createMapReduce(HadoopGraph.open(apacheConfiguration), apacheConfiguration), partitionIterator);
        });
        if (mapReduce.getMapKeySort().isPresent()) mapRDD = mapRDD.sortByKey(mapReduce.getMapKeySort().get(), true, 1);
        return mapRDD;
    }

    public static <K, V, OK, OV> JavaPairRDD<OK, OV> executeCombine(final JavaPairRDD<K, V> mapRDD, final Configuration apacheConfiguration) {
        return mapRDD.mapPartitionsToPair(partitionIterator -> {
            HadoopPools.initialize(apacheConfiguration);
            return () -> new CombineIterator<>(MapReduce.<MapReduce<K, V, OK, OV, ?>>createMapReduce(HadoopGraph.open(apacheConfiguration), apacheConfiguration), partitionIterator);
        });
    }

    public static <K, V, OK, OV> JavaPairRDD<OK, OV> executeReduce(final JavaPairRDD<K, V> mapOrCombineRDD, final MapReduce<K, V, OK, OV, ?> mapReduce, final Configuration apacheConfiguration) {
        JavaPairRDD<OK, OV> reduceRDD = mapOrCombineRDD.groupByKey().mapPartitionsToPair(partitionIterator -> {
            HadoopPools.initialize(apacheConfiguration);
            return () -> new ReduceIterator<>(MapReduce.<MapReduce<K, V, OK, OV, ?>>createMapReduce(HadoopGraph.open(apacheConfiguration), apacheConfiguration), partitionIterator);
        });
        if (mapReduce.getReduceKeySort().isPresent()) {
            reduceRDD = reduceRDD.sortByKey(mapReduce.getReduceKeySort().get(), true, 1);
        }
        return reduceRDD;
    }
}
