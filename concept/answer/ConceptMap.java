/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.concept.answer;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.thing.Thing;
import com.vaticle.typedb.core.concept.type.Type;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typeql.lang.pattern.variable.Reference;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static java.util.Collections.sort;
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
            else throw TypeDBException.of(ILLEGAL_STATE);
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

    public static class Ordered extends ConceptMap implements Comparable<Ordered> {

        private final Comparator conceptsComparator;

        public Ordered(Map<Retrievable, ? extends Concept> concepts, Comparator conceptsComparator) {
            this(concepts, new Explainables(), conceptsComparator);
        }

        public Ordered(Map<Retrievable, ? extends Concept> concepts, Explainables explainables, Comparator conceptsComparator) {
            super(concepts, explainables);
            this.conceptsComparator = conceptsComparator;
        }

        @Override
        public int compareTo(Ordered other) {
            assert conceptsComparator.equals(other.conceptsComparator);
            return conceptsComparator.compare(this, other);
        }

        // TODO what about reverse order
        public static class Comparator implements java.util.Comparator<ConceptMap> {

            private final List<Retrievable> sortVars;
            private final java.util.Comparator<ConceptMap> comparator;

            public Comparator(List<Retrievable> sortVars, java.util.Comparator<ConceptMap> comparator) {
                this.sortVars = sortVars;
                this.comparator = comparator;
            }

            public static Comparator create(List<Retrievable> sortVars) {
                assert !sortVars.isEmpty();
                Optional<java.util.Comparator<ConceptMap>> comparator = sortVars.stream()
                        .map(var -> java.util.Comparator.comparing((ConceptMap conceptMap) -> conceptMap.get(var), (concept1, concept2) -> {
                            if (concept1.isAttribute() && concept2.isAttribute()) {
                                Attribute att1 = concept1.asAttribute();
                                Attribute att2 = concept2.asAttribute();
                                if (att1.isString()) {
                                    return att1.asString().getValue().compareToIgnoreCase(att2.asString().getValue());
                                } else if (att1.isBoolean()) {
                                    return att1.asBoolean().getValue().compareTo(att2.asBoolean().getValue());
                                } else if (att1.isLong() && att2.isLong()) {
                                    return att1.asLong().getValue().compareTo(att2.asLong().getValue());
                                } else if (att1.isDouble() || att2.isDouble()) {
                                    Double double1 = att1.isLong() ? att1.asLong().getValue() : att1.asDouble().getValue();
                                    Double double2 = att2.isLong() ? att2.asLong().getValue() : att2.asDouble().getValue();
                                    return double1.compareTo(double2);
                                } else if (att1.isDateTime()) {
                                    return (att1.asDateTime().getValue()).compareTo(att2.asDateTime().getValue());
                                } else {
                                    throw TypeDBException.of(ILLEGAL_STATE);
                                }
                            } else if (concept1.isThing() && concept2.isThing()) {
                                return concept1.asThing().compareTo(concept2.asThing());
                            } else if (concept1.isType() && concept2.isType()) {
                                return concept1.asType().compareTo(concept2.asType());
                            } else {
                                throw TypeDBException.of("Concepts '" + concept1 + " and '" + concept2 + "' are not orderable.");
                            }
                        })).reduce(java.util.Comparator::thenComparing);
                return new Comparator(sortVars, comparator.get());
            }

            @Override
            public int compare(ConceptMap o1, ConceptMap o2) {
                return comparator.compare(o1, o2);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Comparator that = (Comparator) o;
                return sortVars.equals(that.sortVars);
            }

            @Override
            public int hashCode() {
                return sortVars.hashCode();
            }
        }
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
