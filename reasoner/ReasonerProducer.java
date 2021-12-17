/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.reasoner;

import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.concurrent.executor.Executors;
import com.vaticle.typedb.core.concurrent.producer.Producer;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.reasoner.reactive.Provider;
import com.vaticle.typedb.core.reasoner.reactive.Sink;
import com.vaticle.typedb.core.reasoner.resolution.ControllerRegistry;
import com.vaticle.typedb.core.reasoner.resolution.framework.Request;
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer;
import com.vaticle.typedb.core.traversal.common.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public class ReasonerProducer implements Producer<ConceptMap> { // TODO: Rename to MatchProducer and create abstract supertype

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerProducer.class);

    private final AtomicInteger required;
    private final AtomicInteger processing;
    private final Options.Query options;
    private final ControllerRegistry controllerRegistry;
    private final ExplainablesManager explainablesManager;
    private final int computeSize;
    private final UUID traceId;
    private final EntryPoint reasonerEntryPoint;
    private boolean done;
    private Queue<ConceptMap> queue;
    private int requestIdCounter;

    // TODO: this class should not be a Producer, it implements a different async processing mechanism
    public ReasonerProducer(Conjunction conjunction, Set<Identifier.Variable.Retrievable> filter, Options.Query options,
                            ControllerRegistry controllerRegistry, ExplainablesManager explainablesManager) {
        this.options = options;
        this.controllerRegistry = controllerRegistry;
        this.explainablesManager = explainablesManager;
        this.queue = null;
        this.done = false;
        this.required = new AtomicInteger();
        this.processing = new AtomicInteger();
        this.reasonerEntryPoint = new EntryPoint();
        this.controllerRegistry.createRoot(conjunction, reasonerEntryPoint);
        this.computeSize = options.parallel() ? Executors.PARALLELISATION_FACTOR * 2 : 1;
        assert computeSize > 0;
        this.requestIdCounter = 0;
        this.traceId = UUID.randomUUID();
        if (options.traceInference()) ResolutionTracer.initialise(options.logsDir());
    }

    public ReasonerProducer(Disjunction disjunction, Set<Identifier.Variable.Retrievable> filter, Options.Query options,
                            ControllerRegistry controllerRegistry, ExplainablesManager explainablesManager) {
        this.options = options;
        this.controllerRegistry = controllerRegistry;
        this.explainablesManager = explainablesManager;
        this.queue = null;
        this.done = false;
        this.required = new AtomicInteger();
        this.processing = new AtomicInteger();
        this.reasonerEntryPoint = new EntryPoint();
        this.computeSize = options.parallel() ? Executors.PARALLELISATION_FACTOR * 2 : 1;
        assert computeSize > 0;
        this.requestIdCounter = 0;
        this.traceId = UUID.randomUUID();
        if (options.traceInference()) ResolutionTracer.initialise(options.logsDir());
    }

    @Override
    public synchronized void produce(Queue<ConceptMap> queue, int request, Executor executor) {
        assert this.queue == null || this.queue == queue;
        this.queue = queue;
        this.required.addAndGet(request);
        int canRequest = computeSize - processing.get();
        int toRequest = Math.min(canRequest, request);
        reasonerEntryPoint.pull();
        processing.addAndGet(toRequest);
    }

    @Override
    public void recycle() {

    }

    /**
     * Essentially the reasoner sink
     */
    public class EntryPoint extends Sink<ConceptMap> {

        @Override
        public void receive(@Nullable Provider<ConceptMap> provider, ConceptMap conceptMap) {
            // if (options.traceInference()) ResolutionTracer.get().finish(answeredRequest);  // TODO: Finish tracing on receive
            isPulling = false;
            if (options.explain() && !conceptMap.explainables().isEmpty()) {
                explainablesManager.setAndRecordExplainables(conceptMap);
            }
            queue.put(conceptMap);
            if (required.decrementAndGet() > 0) pull();
            else processing.decrementAndGet();
        }
        // Trace trace = Trace.create(traceId, requestIdCounter);  // TODO: Trace on pull
        // ResolutionTracer.get().start(visitRequest);

    }

    // note: root resolver calls this single-threaded, so is threads safe
    private void requestFailed(Request failedRequest) {
        // TODO: Add some fail signal
        LOG.trace("Failed to find answer to request {}", failedRequest);
        if (options.traceInference()) ResolutionTracer.get().finish(failedRequest);
        finish();
    }

    private void finish() {
        if (!done) {
            // query is completely terminated
            done = true;
            queue.done();
            required.set(0);
        }
    }

    private void exception(Throwable e) {
        // TODO: Add exception propagation through reactives?
        if (!done) {
            done = true;
            required.set(0);
            queue.done(e);
        }
    }

}
