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

import com.vaticle.typedb.core.concurrent.actor.Actor;
import com.vaticle.typedb.core.reasoner.resolution.answer.AnswerState;

import java.util.Objects;

public class Downstream {
    protected final Actor.Driver<? extends Resolver<?>> sender;
    protected final Actor.Driver<? extends Resolver<?>> receiver;
    protected final AnswerState.Partial<?> partialAnswer;
    protected final int planIndex;

    private final int hash;

    private Downstream(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                       AnswerState.Partial<?> partialAnswer, int planIndex) {
        this.sender = sender;
        this.receiver = receiver;
        this.partialAnswer = partialAnswer;
        this.planIndex = planIndex;
        this.hash = Objects.hash(this.sender, this.receiver, this.partialAnswer);
    }

    public static Downstream create(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                       AnswerState.Partial<?> partialAnswer, int planIndex) {
        return new Downstream(sender, receiver, partialAnswer, planIndex);
    }

    public static Downstream create(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                       AnswerState.Partial<?> partialAnswer) {
        return new Downstream(sender, receiver, partialAnswer, -1);
    }

    public static Downstream of(Request request) {
        return Downstream.create(request.sender(), request.receiver(), request.partialAnswer(), request.planIndex());
    }

    public Request toRequest(ResolutionTracer.TraceId traceId) {
        return Request.create(sender, receiver, traceId, partialAnswer, planIndex);
    }

    public AnswerState.Partial<?> partialAnswer() {
        return partialAnswer;
    }

    public int planIndex() {
        return planIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Downstream downstream = (Downstream) o;
        return Objects.equals(sender, downstream.sender) &&
                Objects.equals(receiver, downstream.receiver) &&
                Objects.equals(partialAnswer, downstream.partialAnswer);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "Downstream{" +
                "sender=" + sender +
                ", receiver=" + receiver +
                ", partial=" + partialAnswer +
                '}';
    }
}
