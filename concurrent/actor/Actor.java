/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.concurrent.actor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class Actor<ACTOR extends Actor<ACTOR>> {

    private static final String ERROR_ACTOR_DRIVER_IS_NULL = "driver() must not be null.";
    private final Driver<ACTOR> driver;
    private final String name;

    public static <NEW_ACTOR extends Actor<NEW_ACTOR>> Driver<NEW_ACTOR> driver(
            Function<Driver<NEW_ACTOR>, NEW_ACTOR> actorFn, EventLoopGroup eventLoopGroup) {
        Driver<NEW_ACTOR> actor = new Driver<>(eventLoopGroup);
        actor.actor = actorFn.apply(actor);
        return actor;
    }

    protected abstract void exception(Throwable e);

    protected Actor(Driver<ACTOR> driver, String name) {
        this.driver = driver;
        this.name = name;
    }

    protected Driver<ACTOR> driver() {
        assert this.driver != null : ERROR_ACTOR_DRIVER_IS_NULL;
        return this.driver;
    }

    public String name() {
        return name;
    }

    public static class Driver<ACTOR extends Actor<ACTOR>> {

        private static final String ERROR_ACTOR_NOT_SETUP =
                "Attempting to access the Actor, but it is not yet setup. Are you trying to send a message to yourself within the constructor?";

        private ACTOR actor;
        private final EventLoopGroup eventLoopGroup;
        private final EventLoop eventLoop;

        private Driver(EventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = eventLoopGroup;
            this.eventLoop = eventLoopGroup.assignEventLoop();
        }

        // TODO: do not use this method - any usages should be removed ASAP
        public ACTOR actor() {
            return actor;
        }

        public String name() {
            return actor.name();
        }

        public void execute(Consumer<ACTOR> job) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            eventLoop.schedule(() -> job.accept(actor), actor::exception);
        }

        public CompletableFuture<Void> complete(Consumer<ACTOR> job) {
            return compute(actor -> {
                job.accept(actor);
                return null;
            });
        }

        public <ANSWER> CompletableFuture<ANSWER> compute(Function<ACTOR, ANSWER> job) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            CompletableFuture<ANSWER> future = new CompletableFuture<>();
            eventLoop.schedule(
                    () -> future.complete(job.apply(actor)),
                    e -> {
                        actor.exception(e);
                        future.completeExceptionally(e);
                    }
            );
            return future;
        }

        public EventLoop.Cancellable schedule(long deadlineMs, Consumer<ACTOR> job) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            return eventLoop.schedule(deadlineMs, () -> job.accept(actor), actor::exception);
        }

        public EventLoopGroup eventLoopGroup() {
            return eventLoopGroup;
        }

        public EventLoop eventLoop() {
            return eventLoop;
        }
    }
}
