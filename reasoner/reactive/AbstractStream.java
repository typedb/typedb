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

package com.vaticle.typedb.core.reasoner.reactive;

import com.vaticle.typedb.core.reasoner.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.reactive.common.ReactiveActions;
import com.vaticle.typedb.core.reasoner.reactive.common.SubscriberRegistry;

public abstract class AbstractStream<INPUT, OUTPUT> extends AbstractReactive implements Reactive.Stream<INPUT, OUTPUT> {

    private final SubscriberRegistry<OUTPUT> subscriberRegistry;
    private final PublisherRegistry<INPUT> publisherRegistry;
    protected final ReactiveActions.SubscriberActions<INPUT> subscriberActions;
    protected final ReactiveActions.PublisherActions<OUTPUT> publisherActions;

    protected AbstractStream(AbstractReactiveBlock<?, ?, ?, ?> reactiveBlock,
                             SubscriberRegistry<OUTPUT> subscriberRegistry,
                             PublisherRegistry<INPUT> publisherRegistry) {
        super(reactiveBlock);
        this.subscriberRegistry = subscriberRegistry;
        this.publisherRegistry = publisherRegistry;
        this.subscriberActions = new SubscriberActionsImpl<>(this, reactiveBlock.context());
        this.publisherActions = new PublisherActionsImpl<>(this, reactiveBlock.context());
    }

    public SubscriberRegistry<OUTPUT> subscriberRegistry() { return subscriberRegistry; }

    public PublisherRegistry<INPUT> publisherRegistry() {
        return publisherRegistry;
    }

    public void propagatePull(Publisher<INPUT> publisher) {
        publisherRegistry().setPulling(publisher);
        publisher.pull(this);
    }

    @Override
    public void registerPublisher(Publisher<INPUT> publisher) {
        publisherRegistry().add(publisher);
        subscriberActions.registerPath(publisher);
        if (subscriberRegistry().anyPulling() && publisherRegistry().setPulling(publisher)) propagatePull(publisher);
    }

    @Override
    public void registerSubscriber(Subscriber<OUTPUT> subscriber) {
        subscriberRegistry().addSubscriber(subscriber);
        subscriber.registerPublisher(this);  // TODO: Bad to have this mutual registering in one method call, it's unclear
    }
}
