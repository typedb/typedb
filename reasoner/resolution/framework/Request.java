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
 *
 */

package grakn.core.reasoner.resolution.framework;

import grakn.core.concurrent.actor.Actor;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.answer.AnswerState.Partial;
import grakn.core.reasoner.resolution.resolver.NegationResolver;
import grakn.core.reasoner.resolution.resolver.Root;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static grakn.common.collection.Collections.list;

public class Request {

    // TODO: add compute mode: single vs all (for negation vs regular)

    private final Path path;
    private final Partial<?> partialAnswer;
    private final int planIndex;

    private final int hash;

    private Request(Path path,
                    Partial<?> startingConcept,
                    int planIndex) {
        this.path = path;
        this.partialAnswer = startingConcept;
        this.planIndex = planIndex;
        this.hash = Objects.hash(path, partialAnswer);
    }

    public static Request create(Path path, Partial<?> startingConcept, int planIndex) {
        return new Request(path, startingConcept, planIndex);
    }

    public static Request create(Path path, Partial<?> startingConcept) {
        // Set the planIndex to -1 since it is unused in this case
        return new Request(path, startingConcept, -1);
    }

    public Path path() {
        return path;
    }

    public int planIndex() {
        return planIndex;
    }

    @Nullable
    public Actor<? extends Resolver<?>> sender() {
        if (path.path.size() < 2) {
            return null;
        }
        return path.path.get(path.path.size() - 2).resolver;
    }

    public Actor<? extends Resolver<?>> receiver() {
        return path.path.get(path.path.size() - 1).resolver;
    }

    public Partial<?> partialAnswer() {
        return partialAnswer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(path, request.path) &&
                Objects.equals(partialAnswer, request.partialAnswer());
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "Request{" +
                "path=" + path +
                ", partial=" + partialAnswer +
                ", partialDerivation=" + partialAnswer.derivation() +
                '}';
    }

    public AnswerState.Derivation partialResolutions() {
        return partialAnswer.derivation();
    }

    // TODO delete path
    public static class Path {

        private final List<VisitedResolver> path;

        public Path(Actor<? extends Resolver<?>> sender, AnswerState answerState) {
            this(list(new VisitedResolver(sender, answerState)));
        }

        public Path(List<VisitedResolver> path) {
            this.path = path;
        }

        public Path append(Actor<? extends Resolver<?>> actor, AnswerState answerState) {
            List<VisitedResolver> appended = new ArrayList<>(path);
            appended.add(new VisitedResolver(actor, answerState));
            return new Path(appended);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Path other = (Path) o;
            return Objects.equals(path, other.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path);
        }

        public Actor<? extends Resolver<?>> root() {
            assert path.get(0).resolver.state instanceof Root || path.get(0).resolver.state instanceof NegationResolver;
            return path.get(0).resolver;
        }

        /**
         * To distinguish between visiting a resolver with two different unifiers,
         */
        private static class VisitedResolver {

            final Actor<? extends Resolver<?>> resolver;
            AnswerState answerState;

            public VisitedResolver(Actor<? extends Resolver<?>> resolver, AnswerState answerState) {
                this.resolver = resolver;
                this.answerState = answerState;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                final VisitedResolver that = (VisitedResolver) o;
                return Objects.equals(resolver, that.resolver) &&
                        Objects.equals(answerState, that.answerState);
            }

            @Override
            public int hashCode() {
                return Objects.hash(resolver, answerState);
            }
        }
    }
}
