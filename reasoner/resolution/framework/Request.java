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

package com.vaticle.typedb.core.reasoner.resolution.framework;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState.Partial;

import javax.annotation.Nullable;
import java.util.Objects;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class Request {

    protected final Actor.Driver<? extends Resolver<?>> sender;
    protected final Actor.Driver<? extends Resolver<?>> receiver;
    protected final Partial<?> partialAnswer;
    protected final int planIndex;

    private final int hash;

    private Request(@Nullable Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                    Partial<?> partialAnswer, int planIndex) {
        this.sender = sender;
        this.receiver = receiver;
        this.partialAnswer = partialAnswer;
        this.planIndex = planIndex;
        this.hash = Objects.hash(this.sender, this.receiver, this.partialAnswer);
    }

    public static Request create(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver, Partial<?> partialAnswer, int planIndex) {
        return new Request(sender, receiver, partialAnswer, planIndex);
    }

    public static Request create(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver, Partial<?> partialAnswer) {
        return new Request(sender, receiver, partialAnswer, -1);
    }

    public static Request create(Actor.Driver<? extends Resolver<?>> receiver, Partial<?> partialAnswer) {
        return new Request(null, receiver, partialAnswer, -1);
    }

    public Actor.Driver<? extends Resolver<?>> receiver() {
        return receiver;
    }

    public Actor.Driver<? extends Resolver<?>> sender() {
        return sender;
    }

    public Partial<?> partialAnswer() {
        return partialAnswer;
    }

    public int planIndex() {
        return planIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(sender, request.sender) &&
                Objects.equals(receiver, request.receiver) &&
                Objects.equals(partialAnswer, request.partialAnswer());
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "Request{" +
                "sender=" + sender +
                ", receiver=" + receiver +
                ", partial=" + partialAnswer +
                '}';
    }

    public boolean isToSubsumed() {
        return false;
    }

    public ToSubsumed asToSubsumed() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    public boolean isToSubsumer() {
        return false;
    }

    public ToSubsumer asToSubsumer() {
        throw TypeDBException.of(ILLEGAL_STATE);
    }

    public static class ToSubsumed extends Request {

        private final Actor.Driver<? extends Resolver<?>> subsumer;

        private ToSubsumed(@Nullable Actor.Driver<? extends Resolver<?>> sender,
                           Actor.Driver<? extends Resolver<?>> receiver,
                           @Nullable Actor.Driver<? extends Resolver<?>> subsumer, Partial<?> partialAnswer,
                           int planIndex) {
            super(sender, receiver, partialAnswer, planIndex);
            this.subsumer = subsumer;
        }

        public static Request create(Actor.Driver<? extends Resolver<?>> sender,
                                     Actor.Driver<? extends Resolver<?>> receiver,
                                     Actor.Driver<? extends Resolver<?>> subsumer, Partial<?> partialAnswer) {
            return new ToSubsumed(sender, receiver, subsumer, partialAnswer, -1);
        }

        public Actor.Driver<? extends Resolver<?>> subsumer() {
            return subsumer;
        }

        @Override
        public boolean isToSubsumed() {
            return true;
        }

        @Override
        public ToSubsumed asToSubsumed() {
            return this;
        }

    }

    public static class ToSubsumer extends Request {

        private final ToSubsumed toSubsumed;

        private ToSubsumer(@Nullable Actor.Driver<? extends Resolver<?>> sender,
                           Actor.Driver<? extends Resolver<?>> receiver,
                           ToSubsumed toSubsumed, Partial<?> partialAnswer, int planIndex) {
            super(sender, receiver, partialAnswer, planIndex);
            this.toSubsumed = toSubsumed;
        }

        public static ToSubsumer create(@Nullable Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<?
                extends Resolver<?>> receiver, ToSubsumed toSubsumed, Partial<?> partialAnswer) {
            return new ToSubsumer(sender, receiver, toSubsumed, partialAnswer, -1);
        }

        public ToSubsumed toSubsumed() {
            return toSubsumed;
        }

        @Override
        public boolean isToSubsumer() {
            return true;
        }

        @Override
        public ToSubsumer asToSubsumer() {
            return this;
        }

    }

}
