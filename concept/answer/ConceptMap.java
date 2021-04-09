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
import grakn.core.common.iterator.FunctionalIterator;
import grakn.core.concept.Concept;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.Type;
import grakn.core.pattern.Conjunction;
import grakn.core.traversal.common.Identifier;
import grakn.core.traversal.common.Identifier.Variable.Retrievable;
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
import static grakn.core.common.iterator.Iterators.link;
import static java.util.Collections.unmodifiableMap;

public class ConceptMap implements Answer {

    private final Map<Retrievable, ? extends Concept> concepts;
    private final Explainables explainables;
    private final int hash;

    public ConceptMap() {
        this(new HashMap<>());
    }

    public ConceptMap(Map<Retrievable, ? extends Concept> concepts) {
        this(concepts, new Explainables());
    }

    public ConceptMap(Map<Retrievable, ? extends Concept> concepts, Explainables explainables) {
        this.concepts = concepts;
        this.explainables = explainables;
        this.hash = Objects.hash(this.concepts, this.explainables);
    }

    public FunctionalIterator<Pair<Retrievable, Concept>> iterator() {
        return iterate(concepts.entrySet()).map(e -> pair(e.getKey(), e.getValue()));
    }

    public boolean contains(String variable) {
        return contains(Reference.name(variable));
    }

    public boolean contains(Reference.Name variable) {
        return concepts.containsKey(Identifier.Variable.of(variable));
    }

    public boolean contains(Retrievable id) {
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

    public Concept get(Retrievable id) {
        return concepts.get(id);
    }

    public Map<Retrievable, ? extends Concept> concepts() {
        return concepts;
    }

    public ConceptMap filter(Set<? extends Retrievable> vars) {
        Map<Retrievable, ? extends Concept> filtered = concepts.entrySet().stream()
                .filter(e -> vars.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return new ConceptMap(filtered);
    }

    public void forEach(BiConsumer<Retrievable, Concept> consumer) {
        concepts.forEach(consumer);
    }

    public <T, U> Map<Retrievable, Either<T, U>> toMap(Function<Type, T> typeFn, Function<Thing, U> thingFn) {
        return toMap(concept -> {
            if (concept.isType()) return Either.first(typeFn.apply(concept.asType()));
            else if (concept.isThing()) return Either.second(thingFn.apply(concept.asThing()));
            else throw GraknException.of(ILLEGAL_STATE);
        });
    }

    public <T> Map<Retrievable, T> toMap(Function<Concept, T> conceptFn) {
        Map<Retrievable, T> map = new HashMap<>();
        iterate(concepts.entrySet()).forEachRemaining(e -> map.put(e.getKey(), conceptFn.apply(e.getValue())));
        return map;
    }

    public Explainables explainables() {
        return explainables;
    }

    public ConceptMap withExplainableConcept(Retrievable id, Conjunction conjunction) {
        assert concepts.get(id).isRelation() || concepts.get(id).isAttribute();
        if (concepts.get(id).isRelation()) {
            HashMap<Retrievable, Explainable> clone = new HashMap<>(explainables.explainableRelations);
            clone.put(id, Explainable.unidentified(conjunction));
            return new ConceptMap(
                    concepts,
                    new Explainables(unmodifiableMap(clone), explainables.explainableRelations, explainables.explainableOwnerships)
            );
        } else {
            HashMap<Retrievable, Explainable> clone = new HashMap<>(explainables.explainableAttributes);
            clone.put(id, Explainable.unidentified(conjunction));
            return new ConceptMap(
                    concepts,
                    new Explainables(explainables.explainableRelations, unmodifiableMap(clone), explainables.explainableOwnerships)
            );
        }
    }

    public ConceptMap withExplainableAttrOwnership(Retrievable owner, Retrievable attribute, Conjunction conjunction) {
        Map<Pair<Retrievable, Retrievable>, Explainable> explainableAttributeOwnershipsClone = new HashMap<>(explainables.explainableOwnerships);
        explainableAttributeOwnershipsClone.put(new Pair<>(owner, attribute), Explainable.unidentified(conjunction));
        return new ConceptMap(
                concepts,
                new Explainables(explainables.explainableRelations, explainables.explainableAttributes, unmodifiableMap(explainableAttributeOwnershipsClone))
        );
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
        return concepts.equals(that.concepts) && explainables.equals(that.explainables);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    public static class Explainables {

        private final Map<Retrievable, Explainable> explainableRelations;
        private final Map<Retrievable, Explainable> explainableAttributes;
        private final Map<Pair<Retrievable, Retrievable>, Explainable> explainableOwnerships;

        public Explainables() {
            this(unmodifiableMap(new HashMap<>()), unmodifiableMap(new HashMap<>()), unmodifiableMap(new HashMap<>()));
        }

        public Explainables(Map<Retrievable, Explainable> explainableRelations,
                            Map<Retrievable, Explainable> explainableAttributes,
                            Map<Pair<Retrievable, Retrievable>, Explainable> explainableOwnerships) {
            this.explainableRelations = explainableRelations;
            this.explainableAttributes = explainableAttributes;
            this.explainableOwnerships = explainableOwnerships;
        }

        public FunctionalIterator<Explainable> iterator() {
            return link(iterate(explainableRelations.values()), iterate(explainableAttributes.values()), iterate(explainableOwnerships.values()));
        }

        public Map<Retrievable, Explainable> relations() {
            return explainableRelations;
        }

        public Map<Retrievable, Explainable> attributes() {
            return explainableAttributes;
        }

        public Map<Pair<Retrievable, Retrievable>, Explainable> ownerships() {
            return explainableOwnerships;
        }

        public boolean isEmpty() {
            return explainableRelations.isEmpty() && explainableAttributes.isEmpty() && explainableOwnerships.isEmpty();
        }

        public Explainables merge(Explainables explainables) {
            Map<Retrievable, Explainable> relations = new HashMap<>(this.explainableRelations);
            Map<Retrievable, Explainable> attributes = new HashMap<>(this.explainableAttributes);
            Map<Pair<Retrievable, Retrievable>, Explainable> ownerships = new HashMap<>((this.explainableOwnerships));
            relations.putAll(explainables.explainableRelations);
            attributes.putAll(explainables.explainableAttributes);
            ownerships.putAll(explainables.explainableOwnerships);
            return new Explainables(relations, attributes, ownerships);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Explainables that = (Explainables) o;
            return explainableRelations.equals(that.explainableRelations) &&
                    explainableAttributes.equals(that.explainableAttributes) &&
                    explainableOwnerships.equals(that.explainableOwnerships);
        }

        @Override
        public int hashCode() {
            return Objects.hash(explainableRelations, explainableAttributes, explainableOwnerships);
        }

    }

    public static class Explainable {

        public static long NOT_IDENTIFIED = -1L;

        private final Conjunction conjunction;
        private long id;

        private Explainable(Conjunction conjunction, long id) {
            this.conjunction = conjunction;
            this.id = id;
        }

        static Explainable unidentified(Conjunction conjunction) {
            return new Explainable(conjunction, NOT_IDENTIFIED);
        }

        public void setId(long id) {
            this.id = id;
        }

        public Conjunction conjunction() {
            return conjunction;
        }

        public long id() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Explainable that = (Explainable) o;
            return conjunction == that.conjunction; // exclude ID as it changes
        }

        @Override
        public int hashCode() {
            return Objects.hash(conjunction); // exclude ID as it changes
        }
    }
}
