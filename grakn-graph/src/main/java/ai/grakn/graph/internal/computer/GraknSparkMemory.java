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

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.util.Rule;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MemoryHelper;
import org.apache.tinkerpop.gremlin.spark.process.computer.RuleAccumulator;
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
 * @author Jason Liu
 * @author Marko A. Rodriguez
 */
public class GraknSparkMemory implements Memory.Admin, Serializable {

    private static final long serialVersionUID = -3877367965011858056L;

    public final Set<String> memoryKeys = new HashSet<>();
    private final AtomicInteger iteration = new AtomicInteger(0);   // do these need to be atomics?
    private final AtomicLong runtime = new AtomicLong(0l);
    private final Map<String, Accumulator<Rule>> memory = new HashMap<>();
    private Broadcast<Map<String, Object>> broadcast;
    private boolean inTask = false;

    public GraknSparkMemory(final VertexProgram<?> vertexProgram,
                            final Set<MapReduce> mapReducers,
                            final JavaSparkContext sparkContext) {
        if (null != vertexProgram) {
            for (final String key : vertexProgram.getMemoryComputeKeys()) {
                MemoryHelper.validateKey(key);
                this.memoryKeys.add(key);
            }
        }
        for (final MapReduce mapReduce : mapReducers) {
            this.memoryKeys.add(mapReduce.getMemoryKey());
        }
        for (final String key : this.memoryKeys) {
            this.memory.put(key,
                    sparkContext.accumulator(new Rule(Rule.Operation.NO_OP, null), key, new RuleAccumulator()));
        }
        this.broadcast = sparkContext.broadcast(new HashMap<>());
    }

    @Override
    public Set<String> keys() {
        if (this.inTask) {
            return this.broadcast.getValue().keySet();
        } else {
            final Set<String> trueKeys = new HashSet<>();
            this.memory.forEach((key, value) -> {
                if (value.value().getObject() != null) trueKeys.add(key);
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
        final R r = this.getValue(key);
        if (null == r) {
            throw Memory.Exceptions.memoryDoesNotExist(key);
        } else {
            return r;
        }
    }

    @Override
    public void incr(final String key, final long delta) {
        checkKeyValue(key, delta);
        if (this.inTask) {
            this.memory.get(key).add(new Rule(Rule.Operation.INCR, delta));
        } else {
            this.memory.get(key).setValue(new Rule(Rule.Operation.INCR, this.<Long>getValue(key) + delta));
        }
    }

    @Override
    public void and(final String key, final boolean bool) {
        checkKeyValue(key, bool);
        if (this.inTask) {
            this.memory.get(key).add(new Rule(Rule.Operation.AND, bool));
        } else {
            this.memory.get(key).setValue(new Rule(Rule.Operation.AND, this.<Boolean>getValue(key) && bool));
        }
    }

    @Override
    public void or(final String key, final boolean bool) {
        checkKeyValue(key, bool);
        if (this.inTask) {
            this.memory.get(key).add(new Rule(Rule.Operation.OR, bool));
        } else {
            this.memory.get(key).setValue(new Rule(Rule.Operation.OR, this.<Boolean>getValue(key) || bool));
        }
    }

    @Override
    public void set(final String key, final Object value) {
        checkKeyValue(key, value);
        if (this.inTask) {
            this.memory.get(key).add(new Rule(Rule.Operation.SET, value));
        } else {
            this.memory.get(key).setValue(new Rule(Rule.Operation.SET, value));
        }
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this);
    }

    protected void setInTask(final boolean inTask) {
        this.inTask = inTask;
    }

    protected void broadcastMemory(final JavaSparkContext sparkContext) {
        this.broadcast.destroy(true); // do we need to block?
        final Map<String, Object> toBroadcast = new HashMap<>();
        this.memory.forEach((key, rule) -> {
            if (null != rule.value().getObject()) {
                toBroadcast.put(key, rule.value().getObject());
            }
        });
        this.broadcast = sparkContext.broadcast(toBroadcast);
    }

    private void checkKeyValue(final String key, final Object value) {
        if (!this.memoryKeys.contains(key)) {
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
        }
        MemoryHelper.validateValue(value);
    }

    private <R> R getValue(final String key) {
        return this.inTask ? (R) this.broadcast.value().get(key) : (R) this.memory.get(key).value().getObject();
    }
}
