/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
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
