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
import com.vaticle.typedb.core.reasoner.resolution.framework.ResolutionTracer.Trace;
import com.vaticle.typedb.core.reasoner.resolution.framework.Response.Blocked.Cycle;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

public interface Request {

    Visit visit();

    Trace trace();

    class Visit implements Request {

        protected final Actor.Driver<? extends Resolver<?>> sender;
        protected final Actor.Driver<? extends Resolver<?>> receiver;
        protected final AnswerState.Partial<?> partialAnswer;
        protected final int planIndex;
        private final Trace trace;

        private Visit(@Nullable Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                        AnswerState.Partial<?> partialAnswer, int planIndex, Trace trace) {
            this.sender = sender;
            this.receiver = receiver;
            this.partialAnswer = partialAnswer;
            this.planIndex = planIndex;
            this.trace = trace;
        }

        public Actor.Driver<? extends Resolver<?>> receiver() {
            return receiver;
        }

        public Actor.Driver<? extends Resolver<?>> sender() {
            return sender;
        }

        public AnswerState.Partial<?> partialAnswer() {
            return partialAnswer;
        }

        public Template template() {
            return Template.create(sender(), receiver(), partialAnswer(), planIndex());
        }

        @Override
        public Visit visit() {
            return this;
        }

        @Override
        public Trace trace() {
            return trace;
        }

        public int planIndex() {
            return planIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Visit visit = (Visit) o;
            return Objects.equals(sender, visit.sender) &&
                    Objects.equals(receiver, visit.receiver) &&
                    Objects.equals(partialAnswer, visit.partialAnswer) &&
                    Objects.equals(trace, visit.trace);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.sender, this.receiver, this.partialAnswer, this.trace);
        }

        @Override
        public String toString() {
            return "Visit{" +
                    "sender=" + sender +
                    ", receiver=" + receiver +
                    ", partial=" + partialAnswer +
                    ", trace=" + trace +
                    '}';
        }

    }

    class Revisit implements Request {

        private final Visit visit;
        private final Set<Cycle> cycles;

        protected Revisit(Visit visit, Set<Cycle> cycles) {
            this.visit = visit;
            this.cycles = cycles;
        }

        public static Revisit create(Visit visit, Set<Cycle> cycles) {
            return new Revisit(visit, cycles);
        }

        @Override
        public Visit visit() {
            return visit;
        }

        @Override
        public Trace trace() {
            return visit().trace;
        }

        public Set<Cycle> cycles() {
            return cycles;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Revisit revisit = (Revisit) o;
            return visit.equals(revisit.visit) &&
                    cycles.equals(revisit.cycles);
        }

        @Override
        public int hashCode() {
            return Objects.hash(visit, cycles);
        }

        @Override
        public String toString() {
            return "Revisit{" +
                    "visit=" + visit +
                    ", cycles=" + cycles +
                    '}';
        }
    }

    class Template {
        protected final Actor.Driver<? extends Resolver<?>> sender;
        protected final Actor.Driver<? extends Resolver<?>> receiver;
        protected final AnswerState.Partial<?> partialAnswer;
        protected final int planIndex;

        private final int hash;

        protected Template(@Nullable Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                           AnswerState.Partial<?> partialAnswer, int planIndex) {
            this.sender = sender;
            this.receiver = receiver;
            this.partialAnswer = partialAnswer;
            this.planIndex = planIndex;
            this.hash = Objects.hash(this.sender, this.receiver, this.partialAnswer, this.planIndex);
        }

        public static Template create(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                                      AnswerState.Partial<?> partialAnswer, int planIndex) {
            return new Template(sender, receiver, partialAnswer, planIndex);
        }

        public static Template create(Actor.Driver<? extends Resolver<?>> sender, Actor.Driver<? extends Resolver<?>> receiver,
                                      AnswerState.Partial<?> partialAnswer) {
            return new Template(sender, receiver, partialAnswer, -1);
        }

        public static Template create(Actor.Driver<? extends Resolver<?>> receiver, AnswerState.Partial<?> partialAnswer) {
            return new Template(null, receiver, partialAnswer, -1);
        }

        public static Template of(Visit request) {
            return Template.create(request.sender(), request.receiver(), request.partialAnswer(), request.planIndex());
        }

        public Visit createVisit(Trace trace) {
            return new Visit(sender, receiver, partialAnswer, planIndex, trace);
        }

        public Revisit createRevisit(Trace trace, Set<Cycle> cycles) {
            return new Revisit(createVisit(trace), cycles);
        }

        public AnswerState.Partial<?> partialAnswer() {
            return partialAnswer;
        }

        public Actor.Driver<? extends Resolver<?>> receiver() {
            return receiver;
        }

        public Actor.Driver<? extends Resolver<?>> sender() {
            return sender;
        }

        public int planIndex() {
            return planIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Template template = (Template) o;
            return planIndex == template.planIndex &&
                    Objects.equals(sender, template.sender) &&
                    receiver.equals(template.receiver) &&
                    partialAnswer.equals(template.partialAnswer);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return "Template{" +
                    "sender=" + sender +
                    ", receiver=" + receiver +
                    ", partialAnswer=" + partialAnswer +
                    ", planIndex=" + planIndex +
                    '}';
        }
    }
}
