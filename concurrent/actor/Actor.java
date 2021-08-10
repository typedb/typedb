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
 */

package com.vaticle.typedb.core.concurrent.actor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

@ThreadSafe
public abstract class Actor<ACTOR extends Actor<ACTOR>> {

    private static final Logger LOG = LoggerFactory.getLogger(Actor.class);
    private static final String ERROR_ACTOR_DRIVER_IS_NULL = "driver() must not be null.";
    private final Driver<ACTOR> driver;
    private final String name;
    private boolean terminated;

    public static <A extends Actor<A>> Driver<A> driver(Function<Driver<A>, A> actorFn, ActorExecutorGroup service) {
        return new Driver<>(actorFn, service);
    }

    protected abstract void exception(Throwable e);

    protected Actor(Driver<ACTOR> driver, String name) {
        this.driver = driver;
        this.name = name;
        this.terminated = false;
    }

    protected Driver<ACTOR> driver() {
        assert this.driver != null : ERROR_ACTOR_DRIVER_IS_NULL;
        return this.driver;
    }

    public String name() {
        return name;
    }

    public void terminate(Throwable cause) {
        LOG.debug("Actor terminated. ", cause);
        this.terminated = true;
    }

    public boolean isTerminated() { return terminated; }

    public static class Driver<ACTOR extends Actor<ACTOR>> {

        private static final String ERROR_ACTOR_NOT_SETUP =
                "Attempting to access the Actor, but it is not yet setup. " +
                        "Are you trying to send a message to yourself within the constructor?";

        private ACTOR actor;
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

        public String name() {
            return actor.name();
        }

        public void execute(Consumer<ACTOR> consumer) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            executor.submit(() -> consumer.accept(actor), actor::exception);
        }

        public CompletableFuture<Void> complete(Consumer<ACTOR> consumer) {
            return compute(actor -> {
                consumer.accept(actor);
                return null;
            });
        }

        public <ANSWER> CompletableFuture<ANSWER> compute(Function<ACTOR, ANSWER> function) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            CompletableFuture<ANSWER> future = new CompletableFuture<>();
            executor.submit(
                    () -> future.complete(function.apply(actor)),
                    e -> {
                        actor.exception(e);
                        future.completeExceptionally(e);
                    }
            );
            return future;
        }

        public ActorExecutor.FutureTask schedule(Consumer<ACTOR> consumer, long scheduleMillis) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            return executor.schedule(() -> consumer.accept(actor), scheduleMillis, actor::exception);
        }

        public ActorExecutorGroup executorService() {
            return executorService;
        }

        public ActorExecutor executor() {
            return executor;
        }
    }
}
