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

import javax.annotation.CheckReturnValue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

public class Actor<STATE extends Actor.State<STATE>> {
    private static final String ERROR_ACTOR_SELF_IS_NULL = "self() must not be null.";
    private static final String ERROR_ACTOR_STATE_NOT_SETUP =
            "Attempting to access the Actor state, but it is not yet setup. Are you trying to send a message to yourself within the constructor?";

    public STATE state;
    private final EventLoopGroup eventLoopGroup;
    private final EventLoop eventLoop;

    public static <NEW_STATE extends State<NEW_STATE>>
    Actor<NEW_STATE> create(EventLoopGroup eventLoopGroup, Function<Actor<NEW_STATE>, NEW_STATE> stateConstructor) {

        Actor<NEW_STATE> actor = new Actor<>(eventLoopGroup);
        actor.state = stateConstructor.apply(actor);
        return actor;
    }

    private Actor(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
        this.eventLoop = eventLoopGroup.assignEventLoop();
    }

    public void tell(Consumer<STATE> job) {
        assert state != null : ERROR_ACTOR_STATE_NOT_SETUP;
        eventLoop.schedule(() -> job.accept(state), state::exception);
    }

    @CheckReturnValue
    public CompletableFuture<Void> order(Consumer<STATE> job) {
        return ask(state -> {
            job.accept(state);
            return null;
        });
    }

    @CheckReturnValue
    public <ANSWER> CompletableFuture<ANSWER> ask(Function<STATE, ANSWER> job) {
        assert state != null : ERROR_ACTOR_STATE_NOT_SETUP;
        CompletableFuture<ANSWER> future = new CompletableFuture<>();
        eventLoop.schedule(
                () -> future.complete(job.apply(state)),
                e -> {
                    state.exception(e);
                    future.completeExceptionally(e);
                }
        );
        return future;
    }

    public EventLoop.Cancellable schedule(long deadlineMs, Consumer<STATE> job) {
        assert state != null : ERROR_ACTOR_STATE_NOT_SETUP;
        return eventLoop.schedule(deadlineMs, () -> job.accept(state), state::exception);
    }

    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    public EventLoop eventLoop() {
        return eventLoop;
    }

    public static abstract class State<STATE extends State<STATE>> {
        private final Actor<STATE> self;

        protected abstract void exception(Throwable e);

        protected State(Actor<STATE> self) {
            this.self = self;
        }

        protected Actor<STATE> self() {
            assert this.self != null : ERROR_ACTOR_SELF_IS_NULL;
            return this.self;
        }
    }
}
