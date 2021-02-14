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

package grakn.core.concept.answer;

import grakn.common.collection.Either;
import grakn.common.collection.Pair;
import grakn.core.common.exception.GraknException;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.Concept;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.Type;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.Identifier.Variable.Retrieved;
import graql.lang.pattern.variable.Reference;
import graql.lang.pattern.variable.UnboundVariable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static grakn.common.collection.Collections.pair;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public class ConceptMap implements Answer {

    private final Map<Retrieved, ? extends Concept> concepts;
    private final int hash;

    public ConceptMap() {
        this(new HashMap<>());
    }

    public ConceptMap(Map<Retrieved, ? extends Concept> concepts) {
        this.concepts = concepts;
        this.hash = Objects.hash(this.concepts);
    }

    public ResourceIterator<Pair<Retrieved, Concept>> iterator() {
        return iterate(concepts.entrySet()).map(e -> pair(e.getKey(), e.getValue()));
    }

    public boolean contains(String variable) {
        return contains(Reference.name(variable));
    }

    public boolean contains(Reference.Name variable) {
        return concepts.containsKey(Identifier.Variable.of(variable));
    }

    public boolean contains(Retrieved id) {
        return concepts.containsKey(id);
    }

    public Concept get(String variable) {
        return get(Reference.name(variable));
    }

    public Concept get(UnboundVariable variable) {
        if (!variable.reference().isName()) return null;
        else return get(variable.reference().asName());
    }

    public Concept get(Reference.Name variable) {
        return concepts.get(Identifier.Variable.of(variable));
    }

    public Concept get(Retrieved id) {
        return concepts.get(id);
    }

    public Map<Retrieved, ? extends Concept> concepts() { return concepts; }

    public ConceptMap filter(Set<? extends Retrieved> vars) {
        Map<Retrieved, ? extends Concept> filtered = concepts.entrySet().stream()
                .filter(e -> vars.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new ConceptMap(filtered);
    }

    public void forEach(BiConsumer<Retrieved, Concept> consumer) {
        concepts.forEach(consumer);
    }

    public <T, U> Map<Retrieved, Either<T, U>> toMap(Function<Type, T> typeFn, Function<Thing, U> thingFn) {
        return toMap(concept -> {
            if (concept.isType()) return Either.first(typeFn.apply(concept.asType()));
            else if (concept.isThing()) return Either.second(thingFn.apply(concept.asThing()));
            else throw GraknException.of(ILLEGAL_STATE);
        });
    }

    public <T> Map<Retrieved, T> toMap(Function<Concept, T> conceptFn) {
        Map<Retrieved, T> map = new HashMap<>();
        iterate(concepts.entrySet()).forEachRemaining(e -> map.put(e.getKey(), conceptFn.apply(e.getValue())));
        return map;
    }

    @Override
    public String toString() {
        return "ConceptMap{" + concepts + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConceptMap that = (ConceptMap) o;
        return concepts.equals(that.concepts);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
