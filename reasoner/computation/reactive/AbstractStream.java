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

package com.vaticle.typedb.core.reasoner.computation.reactive;

import com.vaticle.typedb.core.reasoner.computation.actor.Processor;
import com.vaticle.typedb.core.reasoner.computation.reactive.utils.ProviderRegistry;
import com.vaticle.typedb.core.reasoner.computation.reactive.utils.ReactiveActions.PublisherActions;
import com.vaticle.typedb.core.reasoner.computation.reactive.utils.ReactiveActions.SubscriberActions;
import com.vaticle.typedb.core.reasoner.computation.reactive.utils.SubscriberRegistry;

public abstract class AbstractStream<INPUT, OUTPUT> extends ReactiveImpl implements Reactive.Stream<INPUT, OUTPUT> {  // TODO: Rename Stream when there's no conflict

    private final SubscriberRegistry<Subscriber<OUTPUT>> subscriberRegistry;
    private final ProviderRegistry<Publisher<INPUT>> providerRegistry;
    protected final SubscriberActions<INPUT> subscriberActions;
    protected final PublisherActions<OUTPUT> providerActions;

    protected AbstractStream(Processor<?, ?, ?, ?> processor,
                             SubscriberRegistry<Subscriber<OUTPUT>> subscriberRegistry,
                             ProviderRegistry<Publisher<INPUT>> providerRegistry) {
        super(processor);
        this.subscriberRegistry = subscriberRegistry;
        this.providerRegistry = providerRegistry;
        this.subscriberActions = new SubscriberActionsImpl<>(this);
        this.providerActions = new PublisherActionsImpl<>(this);
    }

    public SubscriberRegistry<Subscriber<OUTPUT>> subscriberRegistry() { return subscriberRegistry; }

    public ProviderRegistry<Publisher<INPUT>> providerRegistry() {
        return providerRegistry;
    }

    public void propagatePull(Publisher<INPUT> publisher) {
        providerRegistry().setPulling(publisher);
        publisher.pull(this);
    }

    @Override
    public void registerProvider(Publisher<INPUT> publisher) {
        if (providerRegistry().add(publisher)) subscriberActions.registerPath(publisher);
        if (subscriberRegistry().anyPulling() && providerRegistry().setPulling(publisher)) propagatePull(publisher);
    }

    @Override
    public void registerSubscriber(Subscriber<OUTPUT> subscriber) {
        subscriberRegistry().addSubscriber(subscriber);
        subscriber.registerProvider(this);  // TODO: Bad to have this mutual registering in one method call, it's unclear
    }
}
