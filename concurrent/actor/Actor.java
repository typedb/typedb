/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concurrent.actor;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Optional;
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

        // TODO: do not use this method - any usages should be removed ASAP
        public ACTOR actor() {
            return actor;
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

        public void executeNext(Consumer<ACTOR> consumer) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            if (!actor.isTerminated) {
                executor.submitFirst(() -> {
                    if (!actor.isTerminated) consumer.accept(actor);
                }, actor::exception);
            }
        }

        public Optional<CompletableFuture<Void>> complete(Consumer<ACTOR> consumer) {
            return compute(actor -> {
                consumer.accept(actor);
                return null;
            });
        }

        public <ANSWER> Optional<CompletableFuture<ANSWER>> compute(Function<ACTOR, ANSWER> function) {
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
                return Optional.of(future);
            } else return Optional.empty();
        }

        public Optional<ActorExecutor.FutureTask> schedule(Consumer<ACTOR> consumer, long scheduleMillis) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            if (!actor.isTerminated) {
                return Optional.of(executor.schedule(() -> {
                    if (!actor.isTerminated) consumer.accept(actor);
                }, scheduleMillis, actor::exception));
            } else return Optional.empty();
        }

        public ActorExecutorGroup executorService() {
            return executorService;
        }

        public ActorExecutor executor() {
            return executor;
        }
    }
}
