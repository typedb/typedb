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

import grakn.core.common.concurrent.actor.Actor;
import grakn.core.reasoner.resolution.answer.AnswerState;
import grakn.core.reasoner.resolution.resolver.RootResolver;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static grakn.common.collection.Collections.list;

public class Request {
    private final Path path;
    private final AnswerState.DownstreamVars answerBounds;
    private final ResolutionAnswer.Derivation partialDerivation;
    private final int planIndex;

    private Request(Path path,
                   AnswerState.DownstreamVars startingConcept,
                   ResolutionAnswer.Derivation partialDerivation,
                   int planIndex) {
        this.path = path;
        this.answerBounds = startingConcept;
        this.partialDerivation = partialDerivation;
        this.planIndex = planIndex;
    }

    public static Request create(Path path,
                                 AnswerState.DownstreamVars startingConcept,
                                 ResolutionAnswer.Derivation partialDerivation,
                                 int planIndex) {
        return new Request(path, startingConcept, partialDerivation, planIndex);
    }

    public static Request create(Path path,
                                 AnswerState.DownstreamVars startingConcept,
                                 ResolutionAnswer.Derivation partialDerivation) {
        // Set the planIndex to -1 since it is unused in this case
        return new Request(path, startingConcept, partialDerivation, -1);
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
        return path.path.get(path.path.size() - 2);
    }

    public Actor<? extends Resolver<?>> receiver() {
        return path.path.get(path.path.size() - 1);
    }

    public AnswerState.DownstreamVars answerBounds() {
        return answerBounds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return Objects.equals(path, request.path) &&
                Objects.equals(answerBounds, request.answerBounds());
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, answerBounds);
    }

    @Override
    public String toString() {
        return "Request{" +
                "path=" + path +
                ", answerBounds=" + answerBounds +
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

        private Path(List<Actor<? extends Resolver<?>>> path) {
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

        public Actor<RootResolver> root() {
            assert path.get(0).state instanceof RootResolver;
            return (Actor<RootResolver>) path.get(0);
        }
    }
}
