/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner;

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.processor.AbstractProcessor;

public interface ReasonerConsumer<ANSWER> {

    void setRootProcessor(Actor.Driver<? extends AbstractProcessor<?, ANSWER, ?, ?>> rootProcessor);

    void receiveAnswer(ANSWER answer);

    void finish();

    void exception(Throwable e);

}
