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
 */

package grakn.core.reasoner.resolution.answer;

import grakn.common.collection.Pair;
import grakn.core.common.exception.GraknException;
import grakn.core.concept.Concept;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.resolvable.Unifier;
import grakn.core.logic.resolvable.Unifier.Requirements.Instance;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.resolver.ConclusionResolver;
import grakn.core.traversal.common.Identifier;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class AnswerState {
    final ConceptMap conceptMap;
    private final Actor.Driver<? extends Resolver<?>> root;
    final boolean requiresReiteration;

    AnswerState(ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
        this.conceptMap = conceptMap;
        this.root = root;
        this.requiresReiteration = requiresReiteration;
    }

    public abstract ConceptMap conceptMap();

    public boolean requiresReiteration() {
        return requiresReiteration;
    }

    public boolean isIdentity() { return false; }

    public Partial.Identity asIdentity() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Partial.Identity.class));
    }

    public boolean isTop() { return false; }

    public Top asTop() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Partial.Identity.class));
    }

    public boolean isPartial() { return false; }

    public Partial<?> asPartial() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Partial.Filtered.class));
    }

    public Actor.Driver<? extends Resolver<?>> root() {
        return root;
    }

    public static abstract class Top extends AnswerState {

        final Set<Identifier.Variable.Name> getFilter;

        Top(ConceptMap conceptMap, @Nullable Set<Identifier.Variable.Name> getFilter, Actor.Driver<? extends Resolver<?>> root,
            boolean requiresReiteration) {
            super(conceptMap, root, requiresReiteration);
            this.getFilter = getFilter;
        }

        @Override
        public ConceptMap conceptMap() {
            return conceptMap.filter(getFilter);
        }

        public boolean isTop() { return true; }

        public Top asTop() {
            return this;
        }

        public static class Initial extends Top {

            private final int hash;

            Initial(ConceptMap conceptMap, @Nullable Set<Identifier.Variable.Name> getFilter, Actor.Driver<? extends Resolver<?>> root) {
                super(conceptMap, getFilter, root, false);
                this.hash = Objects.hash(conceptMap, getFilter, root);
            }

            public static Initial create(Set<Identifier.Variable.Name> getFilter, Actor.Driver<? extends Resolver<?>> root) {
                return new Initial(new ConceptMap(), getFilter, root);
            }

            public Partial.Identity toDownstream() {
                return Partial.Identity.identity(conceptMap(), this, root(), root());
            }

            Finished finish(ConceptMap conceptMap, Conjunction conjunctionAnswered, Map<Conjunction, Boolean> explainables,
                            boolean requiresReiteration) {
                return new Finished(conceptMap, getFilter, root(), conjunctionAnswered, explainables, requiresReiteration);
            }

            @Override
            public String toString() {
                return "AnswerState.Top.Initial{" +
                        "root=" + root() +
                        ", conceptMap=" + conceptMap +
                        ", filter=" + getFilter +
                        '}';
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Top.Initial top = (Top.Initial) o;
                return Objects.equals(root(), top.root()) &&
                        Objects.equals(conceptMap, top.conceptMap) &&
                        Objects.equals(getFilter, top.getFilter);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        public static class Finished extends Top {

            private final Conjunction conjunctionAnswered;
            private final Map<Conjunction, Boolean> explainables;
            private final int hash;

            Finished(ConceptMap conceptMap, @Nullable Set<Identifier.Variable.Name> getFilter, Actor.Driver<? extends Resolver<?>> root,
                     Conjunction conjunctionAnswered, Map<Conjunction, Boolean> explainables, boolean requiresReiteration) {
                super(conceptMap, getFilter, root, requiresReiteration);
                this.conjunctionAnswered = conjunctionAnswered;
                this.explainables = explainables;
                this.hash = Objects.hash(root, conceptMap, getFilter, this.conjunctionAnswered, explainables);
            }

            @Override
            public String toString() {
                return "AnswerState.Top.Finished{" +
                        "root=" + root() +
                        ", conceptMap=" + conceptMap +
                        ", filter=" + getFilter +
                        ", conjunction=" + conjunctionAnswered +
                        ", explainables=" + explainables +
                        ", requiresReiteration=" + requiresReiteration +
                        '}';
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Top.Finished top = (Top.Finished) o;
                return Objects.equals(root(), top.root()) &&
                        Objects.equals(conceptMap, top.conceptMap) &&
                        Objects.equals(getFilter, top.getFilter) &&
                        Objects.equals(conjunctionAnswered, top.conjunctionAnswered) &&
                        Objects.equals(explainables, top.explainables) &&
                        requiresReiteration == top.requiresReiteration;
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

    }

    public static abstract class Partial<Parent extends AnswerState> extends AnswerState {

        protected final Parent parent;
        private final Actor.Driver<? extends Resolver<?>> resolver; // resolver extending this answer state (eg the receiver)

        public Partial(ConceptMap partialAnswer, Parent parent, Actor.Driver<? extends Resolver<?>> resolver,
                       Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
            super(partialAnswer, root, requiresReiteration);
            this.parent = parent;
            this.resolver = resolver;
        }

        abstract Partial<?> with(ConceptMap conceptMap, boolean requiresReiteration);

        // note: can be any of retrievable, negation, nested disjunction/conjunction...
        public Partial.Filtered filterToDownstream(Set<Identifier.Variable.Retrievable> filter, Actor.Driver<? extends Resolver<?>> nextResolver) {
            return Filtered.filter(this, filter, nextResolver, root());
        }

        public MappedConcludable mapToDownstream(Mapping mapping, Actor.Driver<? extends Resolver<?>> nextResolver) {
            return MappedConcludable.map(this, mapping, nextResolver, root());
        }

        public Optional<Partial.Unified> unifyToDownstream(Unifier unifier, Actor.Driver<ConclusionResolver> nextResolver) {
            return Unified.unify(this, unifier, nextResolver, root());
        }

        public ConceptMap conceptMap() {
            return conceptMap;
        }

        protected Parent parent() {
            return parent;
        }

        public Actor.Driver<? extends Resolver<?>> resolvedBy() {
            return resolver;
        }

        @Override
        public boolean isPartial() { return true; }

        @Override
        public Partial<?> asPartial() {
            return this;
        }

        public boolean isFiltered() { return false; }

        public boolean isMapped() { return false; }

        public boolean isUnified() { return false; }

        public Filtered asFiltered() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Filtered.class));
        }

        public MappedConcludable asMapped() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(MappedConcludable.class));
        }

        public Partial.Unified asUnified() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Partial.Unified.class));
        }

        protected ConceptMap extendAnswer(ConceptMap extension) {
            /*
            We MUST retain initial concepts, and add derived answers afterward. It's possible, and correct,
            that the derived answers overlap but are different: for example, when a subtype is found
            by the derived answer, but the initial already uses the supertype.
             */
            Map<Identifier.Variable.Retrievable, Concept> concepts = new HashMap<>(extension.concepts());
            // add the initial concept map second, to make sure we override and retain all of these
            concepts.putAll(conceptMap().concepts());
            return new ConceptMap(concepts);
        }

        public static class Identity extends Partial<Top.Initial> {

            private final int hash;
            private final Map<Conjunction, Boolean> explainables;

            private Identity(ConceptMap partialAnswer, Top.Initial parent, Actor.Driver<? extends Resolver<?>> resolver, Actor.Driver<? extends Resolver<?>> root,
                             boolean requiresReiteration, Map<Conjunction, Boolean> explainables) {
                super(partialAnswer, parent, resolver, root, requiresReiteration);
                this.explainables = explainables;
                this.hash = Objects.hash(root, resolver, conceptMap, parent);
            }

            static Identity identity(ConceptMap conceptMap, Top.Initial parent, Actor.Driver<? extends Resolver<?>> resolver,
                                     Actor.Driver<? extends Resolver<?>> root) {
                return new Identity(conceptMap, parent, resolver, root, false, new HashMap<>());
            }

            Partial<?> with(ConceptMap extension, boolean requiresReiteration) {
                return new Identity(extendAnswer(extension), parent(), resolvedBy(), root(), requiresReiteration, explainables);
            }

            Partial<?> with(ConceptMap extension, boolean requiresReiteration, Conjunction source, boolean explainable) {
                Map<Conjunction, Boolean> explainablesClone = new HashMap<>(explainables);
                explainablesClone.put(source, explainable);
                return new Identity(extendAnswer(extension), parent(), resolvedBy(), root(), requiresReiteration, explainablesClone);
            }

            public Top.Finished toFinishedTop(Conjunction conjunctionAnswered) {
                return parent().finish(conceptMap(), conjunctionAnswered, explainables, requiresReiteration || parent().requiresReiteration());
            }

            public boolean isIdentity() { return true; }

            public Identity asIdentity() {
                return this;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Identity identity = (Identity) o;
                return Objects.equals(root(), identity.root()) &&
                        Objects.equals(resolvedBy(), identity.resolvedBy()) &&
                        Objects.equals(conceptMap, identity.conceptMap) &&
                        Objects.equals(parent, identity.parent);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        public static class Filtered extends Partial<Partial<?>> {

            private final Set<Identifier.Variable.Retrievable> filter;
            private final int hash;

            private Filtered(ConceptMap filteredConceptMap, Partial<?> parent, Set<Identifier.Variable.Retrievable> filter,
                             Actor.Driver<? extends Resolver<?>> resolver, Actor.Driver<? extends Resolver<?>> root,
                             boolean requiresReiteration) {
                super(filteredConceptMap, parent, resolver, root, requiresReiteration);
                this.filter = filter;
                this.hash = Objects.hash(root, resolver, conceptMap, parent, filter);
            }

            static Filtered filter(Partial<?> parent, Set<Identifier.Variable.Retrievable> filter, Actor.Driver<? extends Resolver<?>> resolver,
                                   Actor.Driver<? extends Resolver<?>> root) {
                return new Filtered(parent.conceptMap().filter(filter), parent, filter, resolver, root, false);
            }

            public Partial<?> toUpstream() {
                if (conceptMap().concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                return parent().with(conceptMap().filter(filter), requiresReiteration || parent().requiresReiteration());
            }

            public Partial<?> aggregateToUpstream(ConceptMap conceptMap) {
                if (conceptMap.concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                return parent().with(conceptMap.filter(filter), requiresReiteration || parent().requiresReiteration());
            }

            @Override
            Filtered with(ConceptMap extension, boolean requiresReiteration) {
                return new Filtered(extendAnswer(extension), parent(), filter, resolvedBy(), root(), requiresReiteration);
            }

            @Override
            public boolean isFiltered() { return true; }

            @Override
            public Filtered asFiltered() { return this; }

            @Override
            public String toString() {
                return "AnswerState.Partial.Filtered{" +
                        "root=" + root() +
                        ", resolver=" + resolvedBy() +
                        ", conceptMap=" + conceptMap() +
                        ", filter=" + filter +
                        '}';
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Filtered filtered = (Filtered) o;
                return Objects.equals(root(), filtered.root()) &&
                        Objects.equals(resolvedBy(), filtered.resolvedBy()) &&
                        Objects.equals(conceptMap, filtered.conceptMap) &&
                        Objects.equals(parent, filtered.parent) &&
                        Objects.equals(filter, filtered.filter);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        public static class MappedConcludable extends Partial<Partial<?>> {

            private final Mapping mapping;
            private final int hash;

            private MappedConcludable(ConceptMap mappedConceptMap, Partial<?> parent, Mapping mapping,
                                      Actor.Driver<? extends Resolver<?>> resolver, Actor.Driver<? extends Resolver<?>> root,
                                      boolean requiresReiteration) {
                super(mappedConceptMap, parent, resolver, root, requiresReiteration);
                this.mapping = mapping;
                this.hash = Objects.hash(root, resolver, conceptMap, mapping, parent);
            }

            static MappedConcludable map(Partial<?> parent, Mapping mapping, Actor.Driver<? extends Resolver<?>> resolver,
                                         Actor.Driver<? extends Resolver<?>> root) {
                ConceptMap mappedConceptMap = mapping.transform(parent.conceptMap());
                return new MappedConcludable(mappedConceptMap, parent, mapping, resolver, root, false);
            }

            public Partial<?> aggregateToUpstream(ConceptMap additionalConcepts) {
                return parent().with(mapping.unTransform(additionalConcepts), requiresReiteration || parent().requiresReiteration());
            }

            public Partial<?> toUpstream() {
                return parent().with(mapping.unTransform(this.conceptMap()), requiresReiteration || parent().requiresReiteration());
            }

            @Override
            protected MappedConcludable with(ConceptMap extension, boolean requiresReiteration) {
                return new MappedConcludable(extendAnswer(extension), parent(), mapping, resolvedBy(), root(), requiresReiteration);

            }

            @Override
            public boolean isMapped() { return true; }

            @Override
            public MappedConcludable asMapped() { return this; }

            @Override
            public String toString() {
                return "AnswerState.Partial.Mapped{" +
                        "root=" + root() +
                        ", resolver=" + resolvedBy() +
                        ", conceptMap=" + conceptMap() +
                        ", mapping=" + mapping +
                        '}';
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                MappedConcludable mapped = (MappedConcludable) o;
                return Objects.equals(root(), mapped.root()) &&
                        Objects.equals(resolvedBy(), mapped.resolvedBy()) &&
                        Objects.equals(conceptMap, mapped.conceptMap) &&
                        Objects.equals(parent, mapped.parent) &&
                        Objects.equals(mapping, mapped.mapping);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        public static class Unified extends Partial<Partial<?>> {

            private final Unifier unifier;
            private final Instance instanceRequirements;
            private final int hash;

            private Unified(ConceptMap unifiedConceptMap, Partial<?> parent, Unifier unifier,
                            Instance instanceRequirements, Actor.Driver<? extends Resolver<?>> resolver,
                            Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                super(unifiedConceptMap, parent, resolver, root, requiresReiteration);
                this.unifier = unifier;
                this.instanceRequirements = instanceRequirements;
                this.hash = Objects.hash(root, resolver, conceptMap, unifier, instanceRequirements, parent);
            }

            static Optional<Partial.Unified> unify(Partial<?> parent, Unifier unifier, Actor.Driver<ConclusionResolver> resolver,
                                                   Actor.Driver<? extends Resolver<?>> root) {
                Optional<Pair<ConceptMap, Instance>> unified = unifier.unify(parent.conceptMap());
                return unified.map(unification -> new Partial.Unified(
                        unification.first(), parent, unifier, unification.second(), resolver, root, false
                ));

            }

            public Optional<Partial<?>> aggregateToUpstream(Map<Identifier.Variable, Concept> concepts) {
                Optional<ConceptMap> unUnified = unifier.unUnify(concepts, instanceRequirements);
                return unUnified.map(ans -> parent().with(ans, true));
            }

            @Override
            public boolean isUnified() { return true; }

            @Override
            public Unified asUnified() { return this; }

            @Override
            Unified with(ConceptMap extension, boolean requiresReiteration ) {
                return new Unified(extendAnswer(extension), parent(), unifier, instanceRequirements, resolvedBy(), root(), requiresReiteration);
            }

            public Unified extend(ConceptMap ans) {
                Map<Identifier.Variable.Retrievable, Concept> extended = new HashMap<>();
                extended.putAll(ans.concepts());
                extended.putAll(conceptMap.concepts());
                return new Unified(new ConceptMap(extended), parent(), unifier, instanceRequirements, resolvedBy(), root(),
                                   requiresReiteration);
            }

            @Override
            public String toString() {
                return "AnswerState.Partial.Unified{" +
                        "root=" + root() +
                        ", resolver=" + resolvedBy() +
                        ", conceptMap=" + conceptMap() +
                        ", unifier=" + unifier +
                        ", instanceRequirements=" + instanceRequirements +
                        '}';
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Unified unified = (Unified) o;
                return Objects.equals(root(), unified.root()) &&
                        Objects.equals(resolvedBy(), unified.resolvedBy()) &&
                        Objects.equals(conceptMap, unified.conceptMap) &&
                        Objects.equals(parent, unified.parent) &&
                        Objects.equals(unifier, unified.unifier) &&
                        Objects.equals(instanceRequirements, unified.instanceRequirements);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }
    }
}
