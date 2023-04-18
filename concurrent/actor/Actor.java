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
 */

package com.vaticle.typedb.core.concurrent.actor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@ThreadSafe
public abstract class Actor<ACTOR extends Actor<ACTOR>> {

    private static final String ERROR_ACTOR_DRIVER_IS_NULL = "driver() must not be null.";
    private final Driver<ACTOR> driver;
    private final Supplier<String> debugName;
    boolean isTerminated;

    public static <A extends Actor<A>> Driver<A> driver(Function<Driver<A>, A> actorFn, ActorExecutorGroup service) {
        return new Driver<>(actorFn, service);
    }

    protected Actor(Driver<ACTOR> driver, Supplier<String> debugName) {
        this.driver = driver;
        this.debugName = debugName;
        this.isTerminated = false;
    }

    public Driver<ACTOR> driver() {
        assert this.driver != null : ERROR_ACTOR_DRIVER_IS_NULL;
        return this.driver;
    }

    protected abstract void exception(Throwable e);

    public void terminate(@Nullable Throwable cause) {
        this.isTerminated = true;
    }

    public Supplier<String> debugName() {
        return debugName;
    }

    public static class Driver<ACTOR extends Actor<ACTOR>> {

        private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

        private static final String ERROR_ACTOR_NOT_SETUP =
                "Attempting to access the Actor, but it is not yet setup. " +
                        "Are you trying to send a message to yourself within the constructor?";

        private final ACTOR actor;
        private final ActorExecutorGroup executorService;
        private final ActorExecutor executor;

        private Driver(Function<Driver<ACTOR>, ACTOR> actorFn, ActorExecutorGroup executorService) {
            this.actor = actorFn.apply(this);
            this.executorService = executorService;
            this.executor = executorService.nextExecutor();
        }

        public Supplier<String> debugName() {
            return actor.debugName();
        }

        public void execute(Consumer<ACTOR> consumer) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            if (!actor.isTerminated) {
                executor.submit(() -> {
                    if (!actor.isTerminated) consumer.accept(actor);
                }, actor::exception);
            }
        }

        public void executePreemptive(Consumer<ACTOR> consumer) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            if (!actor.isTerminated) {
                executor.submitPreemptive(() -> {
                    if (!actor.isTerminated) consumer.accept(actor);
                }, actor::exception);
            }
        }

        public CompletableFuture<Void> complete(Consumer<ACTOR> consumer) {
            return compute(actor -> {
                consumer.accept(actor);
                return null;
            });
        }

        public <ANSWER> CompletableFuture<ANSWER> compute(Function<ACTOR, ANSWER> function) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            if (!actor.isTerminated) {
                CompletableFuture<ANSWER> future = new CompletableFuture<>();
                executor.submit(
                        () -> {
                            if (!actor.isTerminated) future.complete(function.apply(actor));
                        },
                        e -> {
                            actor.exception(e);
                            future.completeExceptionally(e);
                        }
                );
                return future;
            } else return null;
        }

        public ActorExecutor.FutureTask schedule(Consumer<ACTOR> consumer, long scheduleMillis) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            if (!actor.isTerminated) {
                return executor.schedule(() -> {
                    if (!actor.isTerminated) consumer.accept(actor);
                }, scheduleMillis, actor::exception);
            } else return null;
        }

        public ActorExecutorGroup executorService() {
            return executorService;
        }

        public ActorExecutor executor() {
            return executor;
        }
    }
}
