/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 */

package grakn.core.graph.graphdb.database.management;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.schema.SchemaStatus;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;


public abstract class AbstractIndexStatusWatcher<R, S extends AbstractIndexStatusWatcher<R,S>> implements Callable<R> {

    protected final JanusGraph g;
    protected final List<SchemaStatus> statuses;
    protected Duration timeout;
    protected Duration poll;

    public AbstractIndexStatusWatcher(JanusGraph g) {
        this.g = g;
        this.statuses = new ArrayList<>();
        this.statuses.add(SchemaStatus.REGISTERED);
        this.timeout = Duration.ofSeconds(60L);
        this.poll = Duration.ofMillis(500L);
    }

    protected abstract S self();

    /**
     * Set the target index statuses.  {@link #call()} will repeatedly
     * poll the graph passed into this instance during construction to
     * see whether the index (also passed in during construction) has
     * one of the the supplied statuses.
     *
     * @param statuses
     * @return
     */
    public S status(SchemaStatus... statuses) {
        this.statuses.clear();
        this.statuses.addAll(Arrays.asList(statuses));
        return self();
    }

    /**
     * Set the maximum amount of wall clock time that {@link #call()} will
     * wait for the index to reach the target status.  If the index does
     * not reach the target state in this interval, then {@link #call()}
     * will return a report value indicating failure.
     * <p>
     * A negative {@code timeout} is interpreted to mean "wait forever"
     * (no timeout).  In this case, the {@code timeoutUnit} is ignored.
     *
     * @param timeout the time duration scalar
     * @param timeoutUnit the time unit
     * @return this builder
     */
    public S timeout(long timeout, TemporalUnit timeoutUnit) {
        if (0 > timeout) {
            this.timeout = null;
        } else {
            this.timeout = Duration.of(timeout, timeoutUnit);
        }
        return self();
    }

    /**
     * Set the index information polling interval.  {@link #call()} waits
     * at least this long between repeated attempts to read schema information
     * and determine whether the index has reached its target state.
     */
    public S pollInterval(long poll, TemporalUnit pollUnit) {
        Preconditions.checkArgument(0 <= poll);
        this.poll = Duration.of(poll, pollUnit);
        return self();
    }
}

