/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.graql.executor.computer;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.ObjectWritable;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MemoryHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.spark.process.computer.MemoryAccumulator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * This is a modified version of Spark Memory.
 * We change its behaviour so it can work with our own graph computer.
 * </p>
 *
 */
public class GraknSparkMemory implements Memory.Admin, Serializable {

    private static final long serialVersionUID = -3877367965011858056L;

    public final Map<String, MemoryComputeKey> memoryComputeKeys = new HashMap<>();
    private final Map<String, Accumulator<ObjectWritable>> sparkMemory = new HashMap<>();
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0L);
    private Broadcast<Map<String, Object>> broadcast;
    private boolean inExecute = false;

    public GraknSparkMemory(final VertexProgram<?> vertexProgram,
                            final Set<MapReduce> mapReducers,
                            final JavaSparkContext sparkContext) {
        if (null != vertexProgram) {
            for (final MemoryComputeKey key : vertexProgram.getMemoryComputeKeys()) {
                this.memoryComputeKeys.put(key.getKey(), key);
            }
        }
        for (final MapReduce mapReduce : mapReducers) {
            this.memoryComputeKeys.put(
                    mapReduce.getMemoryKey(),
                    MemoryComputeKey.of(mapReduce.getMemoryKey(), Operator.assign, false, false));
        }
        for (final MemoryComputeKey memoryComputeKey : this.memoryComputeKeys.values()) {
            this.sparkMemory.put(
                    memoryComputeKey.getKey(),
                    sparkContext.accumulator(ObjectWritable.empty(), memoryComputeKey.getKey(),
                            new MemoryAccumulator<>(memoryComputeKey)));
        }
        this.broadcast = sparkContext.broadcast(Collections.emptyMap());
    }

    @Override
    public Set<String> keys() {
        if (this.inExecute) {
            return this.broadcast.getValue().keySet();
        } else {
            final Set<String> trueKeys = new HashSet<>();
            this.sparkMemory.forEach((key, value) -> {
                if (!value.value().isEmpty()) {
                    trueKeys.add(key);
                }
            });
            return Collections.unmodifiableSet(trueKeys);
        }
    }

    @Override
    public void incrIteration() {
        this.iteration.getAndIncrement();
    }

    @Override
    public void setIteration(final int iteration) {
        this.iteration.set(iteration);
    }

    @Override
    public int getIteration() {
        return this.iteration.get();
    }

    @Override
    public void setRuntime(final long runTime) {
        this.runtime.set(runTime);
    }

    @Override
    public long getRuntime() {
        return this.runtime.get();
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        if (!this.memoryComputeKeys.containsKey(key)) {
            throw Memory.Exceptions.memoryDoesNotExist(key);
        }
        if (this.inExecute && !this.memoryComputeKeys.get(key).isBroadcast()) {
            throw Memory.Exceptions.memoryDoesNotExist(key);
        }
        final ObjectWritable<R> r = (ObjectWritable<R>) (this.inExecute ?
                this.broadcast.value().get(key) : this.sparkMemory.get(key).value());
        if (null == r || r.isEmpty()) {
            throw Memory.Exceptions.memoryDoesNotExist(key);
        } else {
            return r.get();
        }
    }

    @Override
    public void add(final String key, final Object value) {
        checkKeyValue(key, value);
        if (this.inExecute) {
            this.sparkMemory.get(key).add(new ObjectWritable<>(value));
        } else {
            throw Memory.Exceptions.memoryAddOnlyDuringVertexProgramExecute(key);
        }
    }

    @Override
    public void set(final String key, final Object value) {
        checkKeyValue(key, value);
        if (this.inExecute) {
            throw Memory.Exceptions.memorySetOnlyDuringVertexProgramSetUpAndTerminate(key);
        } else {
            this.sparkMemory.get(key).setValue(new ObjectWritable<>(value));
        }
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this);
    }


    protected void complete() {
        this.memoryComputeKeys.values().stream()
                .filter(MemoryComputeKey::isTransient)
                .forEach(memoryComputeKey -> this.sparkMemory.remove(memoryComputeKey.getKey()));
    }

    public void setInExecute(final boolean inExecute) {
        this.inExecute = inExecute;
    }

    protected void broadcastMemory(final JavaSparkContext sparkContext) {
        this.broadcast.destroy(true); // do we need to block?
        final Map<String, Object> toBroadcast = new HashMap<>();
        this.sparkMemory.forEach((key, object) -> {
            if (!object.value().isEmpty() && this.memoryComputeKeys.get(key).isBroadcast()) {
                toBroadcast.put(key, object.value());
            }
        });
        this.broadcast = sparkContext.broadcast(toBroadcast);
    }

    private void checkKeyValue(final String key, final Object value) {
        if (!this.memoryComputeKeys.containsKey(key)) {
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
        }
        MemoryHelper.validateValue(value);
    }
}
