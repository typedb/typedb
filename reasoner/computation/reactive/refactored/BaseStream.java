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

package com.vaticle.typedb.core.reasoner.computation.reactive.refactored;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.operator.Operator;

import java.util.function.Function;

public class BaseStream<INPUT, OUTPUT> extends AbstractStream<INPUT, OUTPUT> implements Reactive.Stream<INPUT, OUTPUT> {

    private BaseStream(Processor<?, ?, ?, ?> processor,
                       Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator,
                       ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry,
                       ProviderRegistry<Publisher<INPUT>> providerRegistry,
                       ReactiveActions.SubscriberActions<Publisher<INPUT>, INPUT> receiverActions,
                       ReactiveActions.PublisherActions<Subscriber<OUTPUT>, OUTPUT> providerActions,
                       ReactiveActions.StreamActions<Publisher<INPUT>> streamActions) {
        super(processor, operator, receiverRegistry, providerRegistry, receiverActions, providerActions, streamActions);
    }

//        public static <INPUT, OUTPUT> SyncStream<INPUT, OUTPUT> simple(Processor<?, ?, ?, ?> processor,
//                                                                       Operator<INPUT, OUTPUT, Publisher<INPUT>, Subscriber<OUTPUT>> operator) {
//            return new SyncStream<>(processor, operator, new ReceiverRegistry.Single<>(), new ProviderRegistry.Single<>());
//        }

    private void propagatePull(Publisher<INPUT> provider) {
        provider.pull(this);
    }

    @Override
    public void registerReceiver(Subscriber<OUTPUT> subscriber) {
        receiverRegistry().addReceiver(subscriber);
        subscriber.registerProvider(this);  // TODO: Bad to have this mutual registering in one method call, it's unclear
    }

    @Override
    public void registerProvider(Publisher<INPUT> publisher) {
        if (providerRegistry().add(publisher)) receiverActions.registerPath(null);  // TODO: Should pass "this"
        if (receiverRegistry().anyPulling() && providerRegistry().setPulling(publisher)) propagatePull(publisher);
    }

    @Override
    public <MAPPED> Stream<OUTPUT, MAPPED> map(Function<OUTPUT, MAPPED> function) {
        return PublisherHelper.map(processor, this, function);
    }

    @Override
    public <MAPPED> Stream<OUTPUT, MAPPED> flatMap(Function<OUTPUT, FunctionalIterator<MAPPED>> function) {
        return PublisherHelper.flatMap(processor, this, function);
    }

    @Override
    public Stream<OUTPUT, OUTPUT> buffer() {
        return PublisherHelper.buffer(processor, this);
    }

    @Override
    public Stream<OUTPUT, OUTPUT> deduplicate() {
        return PublisherHelper.deduplicate(processor, this);
    }

    @Override
    public void pull(Subscriber<OUTPUT> outputSubscriber) {

    }

    @Override
    public void receive(Publisher<INPUT> inputPublisher, INPUT input) {

    }
}
