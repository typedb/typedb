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

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.computation.actor.Processor;

import java.util.function.Function;
import java.util.function.Supplier;

public interface Reactive {

    Supplier<String> tracingGroupName();

    Identifier identifier();

    interface Identifier extends Reactive {  // TODO: Don't extend Reactive if possible

        String toString();

        Actor.Driver<? extends Processor<?, ?, ?, ?>> processor();
    }

    interface Provider extends Reactive {

        interface Sync<R> extends Provider {

            void pull(Receiver.Sync<R> receiver);

            interface Publisher<T> extends Sync<T> {

                void publishTo(Receiver.Sync.Subscriber<T> subscriber);

                Stream<T,T> findFirst();

                <R> Stream<T, R> map(Function<T, R> function);

                <R> Stream<T,R> flatMapOrRetry(Function<T, FunctionalIterator<R>> function);

                Stream<T, T> buffer();

                Stream<T, T> deduplicate();

            }
        }

        interface Async extends Provider {

            void pull(Reactive.Identifier receiverId);

        }
    }

    interface Receiver extends Reactive {

        interface Sync<R> extends Receiver {

            void receive(Provider.Sync<R> provider, R packet);

            interface Subscriber<T> extends Sync<T> {

                void subscribeTo(Provider.Sync<T> publisher);

            }

            interface Finishable<T> extends Sync<T> {

                void onFinished();

            }
        }

        interface Async<R> extends Receiver {

            void receive(ReactiveIdentifier.Output<R> providerId, R packet);

        }
    }

    interface Stream<INPUT, OUTPUT> extends Receiver.Sync.Subscriber<INPUT>, Provider.Sync.Publisher<OUTPUT> {

    }

}
