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
 */

package grakn.core.reasoner.resolution.answer;

import grakn.core.concept.answer.ConceptMap;
import graql.lang.pattern.variable.Reference;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class Unifier extends VariableTransformer {

    private final Map<Reference.Name, Set<Reference.Name>> unifier;

    Unifier(Map<Reference.Name, Set<Reference.Name>> unifier) {
        this.unifier = unifier;
    }

    public static Unifier of(ConceptMap conceptMap, Map<Reference.Name, Set<Reference.Name>> unifier) {
        return new Unifier(unifier);
    }

    public static Unifier of(Map<Reference.Name, Set<Reference.Name>> unifier) {
        return new Unifier(unifier);
    }

    public Optional<ConceptMap> transform(ConceptMap toTransform) {
        return null; // TODO
    }

    public Optional<ConceptMap> unTransform(ConceptMap conceptMap) {
        return Optional.empty(); // TODO
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Unifier that = (Unifier) o;
        return unifier.equals(that.unifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unifier);
    }

    @Override
    public boolean isUnifier() {
        return true;
    }

    @Override
    public Unifier asUnifier() {
        return this;
    }
}
