/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.processor.reactive;

import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;

public abstract class AbstractReactive implements Reactive {

    private final AbstractProcessor<?, ?, ?, ?> processor;
    private final Reactive.Identifier identifier;

    AbstractReactive(AbstractProcessor<?, ?, ?, ?> processor) {
        this.processor = processor;
        this.identifier = processor().registerReactive(this);
    }

    @Override
    public Reactive.Identifier identifier() {
        return identifier;
    }

    @Override
    public AbstractProcessor<?, ?, ?, ?> processor() {
        return processor;
    }

}
