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
import grakn.core.reasoner.resolution.resolver.NegationResolver;
import grakn.core.reasoner.resolution.resolver.Root;
import graql.lang.pattern.variable.Reference;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.list;

public class Request {

    // TODO add compute mode: single vs all (for negation vs regular)

    private final Path path;
    private final AnswerState.DownstreamVars partialAnswer;
    private final ResolutionAnswer.Derivation partialDerivation;
    private Set<Reference.Name> answerFilter;
    private final int planIndex;

    private Request(Path path,
                    AnswerState.DownstreamVars startingConcept,
                    ResolutionAnswer.Derivation partialDerivation,
                    int planIndex, @Nullable Set<Reference.Name> answerFilter) {
        this.path = path;
        this.partialAnswer = startingConcept;
        this.partialDerivation = partialDerivation;
        this.answerFilter = answerFilter;
        this.planIndex = planIndex;
    }

    public static Request create(Path path,
                                 AnswerState.DownstreamVars startingConcept,
                                 ResolutionAnswer.Derivation partialDerivation,
                                 int planIndex, @Nullable Set<Reference.Name> answerFilter) {
        return new Request(path, startingConcept, partialDerivation, planIndex, answerFilter);
    }

    public static Request create(Path path,
                                 AnswerState.DownstreamVars startingConcept,
                                 ResolutionAnswer.Derivation partialDerivation,
                                 @Nullable Set<Reference.Name> answerFilter) {
        // Set the planIndex to -1 since it is unused in this case
        return new Request(path, startingConcept, partialDerivation, -1, answerFilter);
    }

    public static Request create(Path path,
                                 AnswerState.DownstreamVars startingConcept,
                                 ResolutionAnswer.Derivation partialDerivation) {
        // Set the planIndex to -1 since it is unused in this case
        return new Request(path, startingConcept, partialDerivation, -1, null);
    }

    public Path path() {
        return path;
    }

    public int planIndex() {
        return planIndex;
    }

    public Optional<Set<Reference.Name>> filter() { return Optional.ofNullable(answerFilter); }

    @Nullable
    public Actor<? extends Resolver<?>> sender() {
        if (path.path.size() < 2) {
            return null;
        }
        return path.path.get(path.path.size() - 2);
    }

    public Actor<? extends Resolver<?>> receiver() {
        return path.path.get(path.path.size() - 1);
    }

    public AnswerState.DownstreamVars partialAnswer() {
        return partialAnswer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(path, request.path) &&
                Objects.equals(partialAnswer, request.partialAnswer()) &&
                Objects.equals(answerFilter, request.answerFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, partialAnswer, answerFilter);
    }

    @Override
    public String toString() {
        return "Request{" +
                "path=" + path +
                ", answerBounds=" + partialAnswer +
                ", partialDerivation=" + partialDerivation +
                '}';
    }

    public ResolutionAnswer.Derivation partialResolutions() {
        return partialDerivation;
    }

    public static class Path {
        final List<Actor<? extends Resolver<?>>> path;

        public Path(Actor<? extends Resolver<?>> sender) {
            this(list(sender));
        }

        public Path(List<Actor<? extends Resolver<?>>> path) {
            assert !path.isEmpty() : "Path cannot be empty";
            this.path = path;
        }

        public Path append(Actor<? extends Resolver<?>> actor) {
            List<Actor<? extends Resolver<?>>> appended = new ArrayList<>(path);
            appended.add(actor);
            return new Path(appended);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Path path1 = (Path) o;
            return Objects.equals(path, path1.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path);
        }

        public Actor<? extends Resolver<?>> root() {
            assert path.get(0).state instanceof Root || path.get(0).state instanceof NegationResolver;
            return path.get(0);
        }
    }
}
