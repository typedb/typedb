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

import com.vaticle.typedb.core.reasoner.computation.reactive.operator.Operator;
import com.vaticle.typedb.core.reasoner.computation.reactive.provider.ReceiverRegistry;

public interface ReactiveActions {

    //TODO: These should probably be provided by the reactive instead of from here
//    Reactive.Identifier<?, ?> identifier();

    // Operator<INPUT, OUTPUT, PROVIDER, RECEIVER> operator();  // TODO: Push this down and be more specific about the type, or just leave it to the reactives to control

//    Processor<?, ?, ?, ?> processor();

    interface ProviderActions<RECEIVER, OUTPUT> extends ReactiveActions {

        void processEffects(Operator.Effects<?> effects);  // TODO: Who needs this? The generic should be empty because in this circumstance no additional providers should be created

        void outputToReceiver(RECEIVER receiver, OUTPUT packet);

        ReceiverRegistry<RECEIVER> receiverRegistry();

        // TODO: map, flatMap, buffer, and deduplicate can go here

    }

    interface ReceiverActions<PROVIDER, INPUT> extends ReactiveActions {

        void registerPath(PROVIDER provider);

        void traceReceive(PROVIDER provider, INPUT packet);

        void rePullProvider(PROVIDER provider);

    }

    interface StreamActions<PROVIDER> extends ReactiveActions {

        void propagatePull(PROVIDER provider);

    }
}
