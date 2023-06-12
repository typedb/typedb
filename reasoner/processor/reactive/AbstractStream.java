/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.reasoner.processor.reactive;

import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;
import com.vaticle.typedb.core.reasoner.processor.reactive.Reactive.Stream;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherDelegate;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.PublisherRegistry;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberDelegate;
import com.vaticle.typedb.core.reasoner.processor.reactive.common.SubscriberRegistry;

public abstract class AbstractStream<INPUT, OUTPUT> extends AbstractReactive implements Stream<INPUT, OUTPUT> {

    private final SubscriberRegistry<OUTPUT> subscriberRegistry;
    private final PublisherRegistry<INPUT> publisherRegistry;
    private final SubscriberDelegate<INPUT> subscriberDelegate;
    private final PublisherDelegate<OUTPUT> publisherDelegate;

    AbstractStream(AbstractProcessor<?, ?, ?, ?> processor,
                   SubscriberRegistry<OUTPUT> subscriberRegistry,
                   PublisherRegistry<INPUT> publisherRegistry) {
        super(processor);
        this.subscriberRegistry = subscriberRegistry;
        this.publisherRegistry = publisherRegistry;
        this.subscriberDelegate = new SubscriberDelegate<>(this, processor.context());
        this.publisherDelegate = new PublisherDelegate<>(this, processor.context());
    }

    protected SubscriberDelegate<INPUT> subscriberDelegate() {
        return subscriberDelegate;
    }

    protected PublisherDelegate<OUTPUT> publisherDelegate() {
        return publisherDelegate;
    }

    protected SubscriberRegistry<OUTPUT> subscriberRegistry() { return subscriberRegistry; }

    protected PublisherRegistry<INPUT> publisherRegistry() {
        return publisherRegistry;
    }

    void propagatePull(Publisher<INPUT> publisher) {
        publisherRegistry().setPulling(publisher);
        publisher.pull(this);
    }

    @Override
    public void registerPublisher(Publisher<INPUT> publisher) {
        if (!publisherRegistry.contains(publisher)) {
            publisherRegistry().add(publisher);
            subscriberDelegate().registerPath(publisher);
        }
        if (subscriberRegistry().anyPulling() && publisherRegistry().setPulling(publisher)) propagatePull(publisher);
    }

    @Override
    public void registerSubscriber(Subscriber<OUTPUT> subscriber) {
        subscriberRegistry().addSubscriber(subscriber);
        subscriber.registerPublisher(this);
    }

}
