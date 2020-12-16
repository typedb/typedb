/*
 * Copyright (C) 2020 Grakn Labs
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

import grakn.common.concurrent.actor.Actor;
import grakn.core.reasoner.resolution.answer.Aggregator;
import grakn.core.reasoner.resolution.resolver.RootResolver;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static grakn.common.collection.Collections.list;

public class Request {
    private final Path path;
    private final Aggregator partialConceptMap;
    private final ResolutionAnswer.Derivation partialDerivation;
    private int iteration; // NOTE: modifiable and not part of equality contract!

    public Request(Path path,
                   Aggregator partialConceptMap,
                   ResolutionAnswer.Derivation partialDerivation) {
        this.path = path;
        this.partialConceptMap = partialConceptMap;
        this.partialDerivation = partialDerivation;
    }

    public Path path() {
        return path;
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

    public Aggregator partialConceptMap() {
        return partialConceptMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        // WARNING: DO NOT INCLUDE ITERATION
        return Objects.equals(path, request.path) &&
                Objects.equals(partialConceptMap, request.partialConceptMap());
    }

    @Override
    public int hashCode() {
        // WARNING: DO NOT INCLUDE ITERATION
        return Objects.hash(path, partialConceptMap);
    }

    @Override
    public String toString() {
        return "Request{" +
                "path=" + path +
                ", partialConceptMap=" + partialConceptMap +
                ", partialDerivation=" + partialDerivation +
                ", iteration(modifiable)=" + iteration +
                '}';
    }

    public ResolutionAnswer.Derivation partialResolutions() {
        return partialDerivation;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    public int getIteration() {
        return iteration;
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
