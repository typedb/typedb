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
 *
 */

package com.vaticle.typedb.core.reasoner.computation.reactive.receiver;

import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.ReasonerConsumer;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.utils.Tracer;

import javax.annotation.Nullable;
import java.util.UUID;

public class EntryPoint extends Sink<ConceptMap> implements Reactive.Receiver.Finishable<ConceptMap> {

    private final String groupName;
    private final UUID traceId = UUID.randomUUID();
    private final ReasonerConsumer reasonerConsumer;
    private boolean isPulling;
    private int traceCounter = 0;

    public EntryPoint(Processor<ConceptMap, ?, ?, ?> processor, ReasonerConsumer reasonerConsumer, String groupName) {
        super(processor);
        this.reasonerConsumer = reasonerConsumer;
        this.groupName = groupName;
        this.isPulling = false;
        reasonerConsumer.setRootProcessor(processor.driver());
        monitor().execute(actor -> actor.registerRoot(processor.driver(), this));
        monitor().execute(actor -> actor.forkFrontier(1, this));
    }

    public void pull() {
        isPulling = true;
        providerRegistry().pullAll();
    }

    @Override
    public void receive(@Nullable Provider<ConceptMap> provider, ConceptMap packet) {
        super.receive(provider, packet);
        isPulling = false;
        reasonerConsumer.receiveAnswer(packet);
        monitor().execute(actor -> actor.consumeAnswer(this));
    }

    @Override
    public void subscribeTo(Provider<ConceptMap> provider) {
        super.subscribeTo(provider);
        if (isPulling) providerRegistry().pull(provider);
    }

    @Override
    public String groupName() {
        return groupName;
    }

    public Tracer.Trace trace() {
        return Tracer.Trace.create(traceId, traceCounter);
    }

    public void exception(Throwable e) {
        reasonerConsumer.exception(e);
    }

    public boolean isPulling() {
        return isPulling;
    }

    @Override
    public void onFinished() {
        reasonerConsumer.answersFinished();
        monitor().execute(actor -> actor.rootFinalised(this));
    }
}
