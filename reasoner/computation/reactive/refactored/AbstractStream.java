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

import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.Reactive;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.receiver.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.ReactiveActions.PublisherActions;
import com.vaticle.typedb.core.reasoner.computation.reactive.refactored.ReactiveActions.SubscriberActions;

public abstract class AbstractStream<INPUT, OUTPUT> extends ReactiveImpl implements Reactive.Stream<INPUT, OUTPUT> {  // TODO: Rename Stream when there's no conflict

    private final ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry;
    private final ProviderRegistry<Publisher<INPUT>> providerRegistry;
    protected final SubscriberActions<INPUT> receiverActions;
    protected final PublisherActions<Subscriber<OUTPUT>, OUTPUT> providerActions;

    protected AbstractStream(Processor<?, ?, ?, ?> processor,
                             ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry,
                             ProviderRegistry<Publisher<INPUT>> providerRegistry) {
        super(processor);
        this.receiverRegistry = receiverRegistry;
        this.providerRegistry = providerRegistry;
        this.receiverActions = new SubscriberActionsImpl<>(this);
        this.providerActions = new PublisherActionsImpl<>(this);
    }

    public ReceiverRegistry<Subscriber<OUTPUT>> receiverRegistry() { return receiverRegistry; }

    public ProviderRegistry<Publisher<INPUT>> providerRegistry() {
        return providerRegistry;
    }

    public void propagatePull(Publisher<INPUT> publisher) {
        publisher.pull(this);
    }

    @Override
    public void registerProvider(Publisher<INPUT> publisher) {
        if (providerRegistry().add(publisher)) receiverActions.registerPath(publisher);
        if (receiverRegistry().anyPulling() && providerRegistry().setPulling(publisher)) propagatePull(publisher);
    }

    @Override
    public void registerReceiver(Subscriber<OUTPUT> subscriber) {
        receiverRegistry().addReceiver(subscriber);
        subscriber.registerProvider(this);  // TODO: Bad to have this mutual registering in one method call, it's unclear
    }
}
