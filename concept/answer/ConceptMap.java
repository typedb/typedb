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

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.thing.Attribute;
import com.vaticle.typedb.core.concept.value.Value;
import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.common.Modifiers;
import com.vaticle.typeql.lang.pattern.variable.Reference;
import com.vaticle.typeql.lang.pattern.variable.UnboundVariable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
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

    public boolean contains(Reference.Name variable) {
        return concepts.containsKey(Identifier.Variable.of(variable));
    }

    public boolean contains(Reference.Name.Concept variable) {
        return concepts.containsKey(Identifier.Variable.of(variable));
    }

    public boolean contains(Reference.Name.Value variable) {
        return concepts.containsKey(Identifier.Variable.of(variable));
    }

    public boolean contains(Retrievable id) {
        return concepts.containsKey(id);
    }

    public boolean containsConcept(String name) {
        return contains(Reference.Name.concept(name));
    }

    public boolean containsValue(String name) {
        return contains(Reference.Name.value(name));
    }

    public Concept get(UnboundVariable variable) {
        if (variable.isNamed()) return get(Identifier.Variable.of(variable.reference().asName()));
        else throw TypeDBException.of(ILLEGAL_STATE);
    }

    public Concept get(Reference.Name variable) {
        return concepts.get(Identifier.Variable.of(variable));
    }

    public Concept get(Retrievable id) {
        return concepts.get(id);
    }

    public Concept getConcept(String variable) {
        return get(Identifier.Variable.namedConcept(variable));
    }

    public Concept getValue(String variable) {
        return get(Identifier.Variable.namedValue(variable));
    }

    public Map<Retrievable, ? extends Concept> concepts() {
        return concepts;
    }

    public Explainables explainables() {
        return explainables;
    }

    public void forEach(BiConsumer<Retrievable, Concept> consumer) {
        concepts.forEach(consumer);
    }

    public ConceptMap filter(Modifiers.Filter filter) {
        return filter(filter.variables());
    }

    public ConceptMap filter(Set<Identifier.Variable.Retrievable> filter) {
        return new ConceptMap(filteredMap(concepts, filter)); // TODO this should include explainables?
    }

    static Map<Retrievable, ? extends Concept> filteredMap(Map<Retrievable, ? extends Concept> concepts, Set<? extends Retrievable> vars) {
        return concepts.entrySet().stream()
                .filter(e -> vars.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public ConceptMap withExplainableConcept(Retrievable id, Conjunction conjunction) {
        assert concepts.get(id).isRelation() || concepts.get(id).isAttribute();
        if (concepts.get(id).isRelation()) {
            return new ConceptMap(concepts, explainables.cloneWithRelation(id, conjunction));
        } else {
            return new ConceptMap(concepts, explainables.cloneWithAttribute(id, conjunction));
        }
    }

    public ConceptMap withExplainableOwnership(Retrievable ownerID, Retrievable attrID, Conjunction conjunction) {
        return new ConceptMap(concepts, explainables.cloneWithOwnership(ownerID, attrID, conjunction));
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

    public static class Sortable extends ConceptMap implements Comparable<Sortable> {

        private final Comparator conceptsComparator;

        public Sortable(Map<Retrievable, ? extends Concept> concepts, Comparator conceptsComparator) {
            this(concepts, new Explainables(), conceptsComparator);
        }

        public Sortable(Map<Retrievable, ? extends Concept> concepts, Explainables explainables, Comparator conceptsComparator) {
            super(concepts, explainables);
            this.conceptsComparator = conceptsComparator;
        }

        @Override
        public Sortable filter(Modifiers.Filter filter) {
            return filter(filter.variables());
        }

        @Override
        public Sortable filter(Set<Retrievable> filter) {
            return new Sortable(filteredMap(concepts(), filter), conceptsComparator); // TODO this should include explainables?
        }

        @Override
        public Sortable withExplainableConcept(Retrievable id, Conjunction conjunction) {
            assert get(id).isRelation() || get(id).isAttribute();
            if (get(id).isRelation()) {
                return new Sortable(concepts(), explainables().cloneWithRelation(id, conjunction), conceptsComparator);
            } else {
                return new Sortable(concepts(), explainables().cloneWithAttribute(id, conjunction), conceptsComparator);
            }
        }

        @Override
        public Sortable withExplainableOwnership(Retrievable ownerID, Retrievable attrID, Conjunction conjunction) {
            return new Sortable(concepts(), explainables().cloneWithOwnership(ownerID, attrID, conjunction), conceptsComparator);
        }

        @Override
        public int compareTo(Sortable other) {
            assert conceptsComparator.equals(other.conceptsComparator);
            return conceptsComparator.compare(this, other);
        }
    }

    public static class Comparator implements java.util.Comparator<ConceptMap> {

        private final Modifiers.Sorting sorting;
        private final java.util.Comparator<ConceptMap> comparator;

        private Comparator(Modifiers.Sorting sorting, java.util.Comparator<ConceptMap> comparator) {
            this.sorting = sorting;
            this.comparator = comparator;
        }

        public static Comparator create(Modifiers.Sorting sorting) {
            assert !sorting.variables().isEmpty();
            Optional<java.util.Comparator<ConceptMap>> comparator = sorting.variables().stream()
                    .map(var -> {
                        java.util.Comparator<ConceptMap> variableComparator = variableComparator(var);
                        return sorting.order(var).get().isAscending() ? variableComparator : variableComparator.reversed();
                    }).reduce(java.util.Comparator::thenComparing);
            return new Comparator(sorting, comparator.get());
        }

        private static java.util.Comparator<ConceptMap> variableComparator(Retrievable var) {
            return java.util.Comparator.comparing(
                    (ConceptMap conceptMap) -> conceptMap.get(var), (concept1, concept2) -> {
                        if (concept1.isValue() && concept2.isValue()) {
                            return compare(concept1.asValue(), concept2.asValue());
                        } else if (concept1.isAttribute() && concept2.isAttribute()) {
                            // TODO: if Attribute were generic we could abstract this away
                            Attribute att1 = concept1.asAttribute();
                            Attribute att2 = concept2.asAttribute();
                            if (att1.isString()) {
                                return Encoding.ValueType.STRING.comparator().compare(att1.asString().getValue(), att2.asString().getValue());
                            } else if (att1.isBoolean()) {
                                return Encoding.ValueType.BOOLEAN.comparator().compare(att1.asBoolean().getValue(), att2.asBoolean().getValue());
                            } else if (att1.isLong() && att2.isLong()) {
                                return Encoding.ValueType.LONG.comparator().compare(att1.asLong().getValue(), att2.asLong().getValue());
                            } else if (att1.isDouble() || att2.isDouble()) {
                                Double double1 = att1.isLong() ? att1.asLong().getValue() : att1.asDouble().getValue();
                                Double double2 = att2.isLong() ? att2.asLong().getValue() : att2.asDouble().getValue();
                                return Encoding.ValueType.DOUBLE.comparator().compare(double1, double2);
                            } else if (att1.isDateTime()) {
                                return Encoding.ValueType.DATETIME.comparator().compare(att1.asDateTime().getValue(), att2.asDateTime().getValue());
                            } else {
                                throw TypeDBException.of(ILLEGAL_STATE);
                            }
                        } else if (concept1.isThing() && concept2.isThing()) {
                            return concept1.asThing().compareTo(concept2.asThing());
                        } else if (concept1.isType() && concept2.isType()) {
                            return concept1.asType().compareTo(concept2.asType());
                        } else {
                            throw TypeDBException.of(ILLEGAL_STATE);
                        }
                    });
        }

        private static <T, U> int compare(Value<T> val1, Value<U> val2) {
            return Encoding.ValueType.compare(val1.valueType(), val1.value(), val2.valueType(), val2.value());
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
            return sorting.equals(that.sorting);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sorting);
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

        Explainables cloneWithRelation(Retrievable id, Conjunction conjunction) {
            HashMap<Retrievable, Explainable> clone = new HashMap<>(explainableRelations);
            clone.put(id, Explainable.unidentified(conjunction));
            return new Explainables(unmodifiableMap(clone), explainableAttributes, explainableOwnerships);
        }

        Explainables cloneWithAttribute(Retrievable id, Conjunction conjunction) {
            HashMap<Retrievable, Explainable> clone = new HashMap<>(explainableAttributes);
            clone.put(id, Explainable.unidentified(conjunction));
            return new Explainables(explainableRelations, unmodifiableMap(clone), explainableOwnerships);
        }

        Explainables cloneWithOwnership(Retrievable ownerID, Retrievable attrID, Conjunction conjunction) {
            Map<Pair<Retrievable, Retrievable>, Explainable> explainableAttributeOwnershipsClone = new HashMap<>(explainableOwnerships);
            explainableAttributeOwnershipsClone.put(new Pair<>(ownerID, attrID), Explainable.unidentified(conjunction));
            return new Explainables(explainableRelations, explainableAttributes, unmodifiableMap(explainableAttributeOwnershipsClone));
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
