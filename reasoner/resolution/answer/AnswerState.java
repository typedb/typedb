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
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.resolver.ConcludableResolver;
import grakn.core.reasoner.resolution.resolver.ConclusionResolver;
import grakn.core.traversal.common.Identifier;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.map;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public abstract class AnswerState {
    final ConceptMap conceptMap;
    private final boolean recordExplanations;
    private final Derivation derivation;
    private final Actor<? extends Resolver<?>> root;
    final boolean requiresReiteration;

    AnswerState(ConceptMap conceptMap, Actor<? extends Resolver<?>> root, boolean requiresReiteration,
                @Nullable Derivation derivation, boolean recordExplanations) {
        this.conceptMap = conceptMap;
        this.root = root;
        this.requiresReiteration = requiresReiteration;
        this.derivation = derivation;
        this.recordExplanations = recordExplanations;
    }

    public boolean recordExplanations() {
        return recordExplanations;
    }

    public abstract ConceptMap conceptMap();

    public Derivation derivation() {
        return derivation;
    }

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

    public Actor<? extends Resolver<?>> root() {
        return root;
    }

    public static class Top extends AnswerState {

        private final Set<Identifier.Variable.Name> getFilter;
        private final int hash;

        Top(ConceptMap conceptMap, @Nullable Set<Identifier.Variable.Name> getFilter,
            Actor<? extends Resolver<?>> root, boolean recordExplanations, boolean requiresReiteration,
            @Nullable Derivation derivation) {
            super(conceptMap, root, requiresReiteration, derivation, recordExplanations);
            this.getFilter = getFilter;
            this.hash = Objects.hash(root, conceptMap, getFilter);
        }

        public static Top initial(Set<Identifier.Variable.Name> getFilter, boolean recordExplanations,
                                  Actor<? extends Resolver<?>> root) {
            Derivation derivation = recordExplanations ? Derivation.EMPTY : null;
            return new Top(new ConceptMap(), getFilter, root, recordExplanations, false, derivation);
        }

        public Partial.Identity toDownstream() {
            return Partial.Identity.identity(conceptMap(), this, root(), root(), recordExplanations());
        }

        Top with(ConceptMap conceptMap, boolean requiresReiteration, @Nullable Derivation derivation) {
            return new Top(conceptMap, getFilter, root(), recordExplanations(), requiresReiteration, derivation);
        }

        @Override
        public String toString() {
            return "AnswerState.Top{" +
                    "root=" + root() +
                    ", conceptMap=" + conceptMap() +
                    ", filter=" + getFilter +
                    '}';
        }

        @Override
        public ConceptMap conceptMap() {
            return conceptMap.filter(getFilter);
        }

        public boolean isTop() { return true; }

        public Top asTop() {
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Top top = (Top) o;
            return Objects.equals(root(), top.root()) &&
                    Objects.equals(conceptMap, top.conceptMap) &&
                    Objects.equals(getFilter, top.getFilter);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static abstract class Partial<Parent extends AnswerState> extends AnswerState {

        protected final Parent parent;
        private final Actor<? extends Resolver<?>> resolver; // resolver extending this answer state (eg the receiver)

        public Partial(ConceptMap partialAnswer, Parent parent, Actor<? extends Resolver<?>> resolver,
                       Actor<? extends Resolver<?>> root, boolean requiresReiteration, @Nullable Derivation derivation,
                       boolean recordExplanations) {
            super(partialAnswer, root, requiresReiteration, derivation, recordExplanations);
            this.parent = parent;
            this.resolver = resolver;
        }

        abstract Partial<?> with(ConceptMap conceptMap, boolean requiresReiteration, Actor<? extends Resolver<?>> derivedBy,
                                 @Nullable Partial<?> extensionState);

        // note: can be any of retrievable, negation, nested disjunction/conjunction...
        public Partial.Filtered filterToDownstream(Set<Identifier.Variable.Retrievable> filter, Actor<? extends Resolver<?>> nextResolver) {
            return Filtered.filter(this, filter, nextResolver, root(), recordExplanations());
        }

        public Partial.Mapped mapToDownstream(Mapping mapping, Actor<? extends Resolver<?>> nextResolver) {
            return Mapped.map(this, mapping, nextResolver, root(), recordExplanations());
        }

        public Optional<Partial.Unified> unifyToDownstream(Unifier unifier, Actor<ConclusionResolver> nextResolver) {
            return Unified.unify(this, unifier, nextResolver, root(), recordExplanations());
        }

        public ConceptMap conceptMap() {
            return conceptMap;
        }

        protected Parent parent() {
            return parent;
        }

        public Actor<? extends Resolver<?>> resolvedBy() {
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

        public Partial.Mapped asMapped() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Partial.Mapped.class));
        }

        public Partial.Unified asUnified() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Partial.Unified.class));
        }

        protected Optional<AnswerState.Derivation> extendDerivation(Actor<? extends Resolver<?>> childDeriver, Partial<?> childPartial) {
            if (recordExplanations()) return Optional.of(derivation().withAnswer(childDeriver, childPartial));
            return Optional.empty();
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

        public static class Identity extends Partial<Top> {

            private final int hash;

            private Identity(ConceptMap partialAnswer, Top parent, Actor<? extends Resolver<?>> resolver, Actor<? extends Resolver<?>> root,
                             boolean requiresReiteration, @Nullable Derivation derivation, boolean recordExplanations) {
                super(partialAnswer, parent, resolver, root, requiresReiteration, derivation, recordExplanations);
                this.hash = Objects.hash(root, resolver, conceptMap, parent);
            }

            static Identity identity(ConceptMap conceptMap, Top parent, Actor<? extends Resolver<?>> resolver, Actor<? extends Resolver<?>> root,
                                     boolean recordExplanations) {
                Derivation derivation = recordExplanations ? new AnswerState.Derivation(new HashMap<>()) : null;
                return new Identity(conceptMap, parent, resolver, root, false, derivation, recordExplanations);
            }

            @Override
            Partial<?> with(ConceptMap extension, boolean requiresReiteration, Actor<? extends Resolver<?>> extendedBy,
                            @Nullable Partial<?> extensionState) {
                Optional<Derivation> extendedDerivation = extendDerivation(extendedBy, extensionState);
                return new Identity(extendAnswer(extension), parent(), resolvedBy(), root(), requiresReiteration, extendedDerivation.orElse(null),
                                    recordExplanations());
            }

            public Top toTop() {
                return parent().asTop().with(conceptMap(), requiresReiteration || parent().requiresReiteration(),
                                             derivation());
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
                             Actor<? extends Resolver<?>> resolver, Actor<? extends Resolver<?>> root,
                             boolean requiresReiteration, @Nullable Derivation derivation, boolean recordExplanations) {
                super(filteredConceptMap, parent, resolver, root, requiresReiteration, derivation, recordExplanations);
                this.filter = filter;
                this.hash = Objects.hash(root, resolver, conceptMap, parent, filter);
            }

            static Filtered filter(Partial<?> parent, Set<Identifier.Variable.Retrievable> filter, Actor<? extends Resolver<?>> resolver,
                                   Actor<? extends Resolver<?>> root, boolean recordExplanations) {
                Derivation derivation = recordExplanations ? new AnswerState.Derivation(new HashMap<>()) : null;
                return new Filtered(parent.conceptMap().filter(filter), parent, filter, resolver, root, false,
                                    derivation, recordExplanations);
            }

            public Partial<?> toUpstream() {
                if (conceptMap().concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                return parent().with(conceptMap().filter(filter), requiresReiteration || parent().requiresReiteration(),
                                     resolvedBy(), this);
            }

            @Override
            Filtered with(ConceptMap extension, boolean requiresReiteration, Actor<? extends Resolver<?>> extendedBy,
                          Partial<?> extensionState) {
                Optional<Derivation> extendedDerivation = extendDerivation(extendedBy, extensionState);
                return new Filtered(extendAnswer(extension), parent(), filter, resolvedBy(), root(), requiresReiteration,
                                    extendedDerivation.orElse(null), recordExplanations());
            }

            @Override
            public boolean isFiltered() { return true; }

            @Override
            public Filtered asFiltered() { return this; }

            @Override
            public String toString() {
                return "AnswerState.Partial.Filtered{" +
                        "root=" + root() +
                        "resolver=" + resolvedBy() +
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

        public static class Mapped extends Partial<Partial<?>> {

            private final Mapping mapping;
            private final int hash;

            private Mapped(ConceptMap mappedConceptMap, Partial<?> parent, Mapping mapping,
                           Actor<? extends Resolver<?>> resolver, Actor<? extends Resolver<?>> root,
                           boolean requiresReiteration, @Nullable Derivation derivation, boolean recordExplanations) {
                super(mappedConceptMap, parent, resolver, root, requiresReiteration, derivation, recordExplanations);
                this.mapping = mapping;
                this.hash = Objects.hash(root, resolver, conceptMap, mapping, parent);
            }

            static Mapped map(Partial<?> parent, Mapping mapping, Actor<? extends Resolver<?>> resolver,
                              Actor<? extends Resolver<?>> root, boolean recordExplanations) {
                ConceptMap mappedConceptMap = mapping.transform(parent.conceptMap());
                Derivation derivation = recordExplanations ? new AnswerState.Derivation(new HashMap<>()) : null;
                return new Mapped(mappedConceptMap, parent, mapping, resolver, root, false, derivation,
                                  recordExplanations);
            }

            public Partial<?> aggregateToUpstream(ConceptMap additionalConcepts) {
                return parent().with(mapping.unTransform(additionalConcepts), requiresReiteration || parent().requiresReiteration(),
                                     resolvedBy(), this);
            }

            public Partial<?> toUpstream() {
                return parent().with(mapping.unTransform(this.conceptMap()), requiresReiteration || parent().requiresReiteration(),
                                     resolvedBy(), this);
            }

            @Override
            protected Mapped with(ConceptMap extension, boolean requiresReiteration, Actor<? extends Resolver<?>> extendedBy,
                                  @Nullable Partial<?> extensionState) {
                Optional<Derivation> derivation = extendDerivation(extendedBy, extensionState);
                assert resolvedBy().state instanceof ConcludableResolver;
                return new Mapped(extendAnswer(extension), parent(), mapping, (Actor<ConcludableResolver>) resolvedBy(), root(), requiresReiteration,
                                  derivation.orElse(null), recordExplanations());
            }

            @Override
            public boolean isMapped() { return true; }

            @Override
            public Mapped asMapped() { return this; }

            @Override
            public String toString() {
                return "AnswerState.Partial.Mapped{" +
                        "root=" + root() +
                        "resolver=" + resolvedBy() +
                        ", conceptMap=" + conceptMap() +
                        ", mapping=" + mapping +
                        '}';
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Mapped mapped = (Mapped) o;
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
                            Instance instanceRequirements, Actor<? extends Resolver<?>> resolver,
                            Actor<? extends Resolver<?>> root, boolean requiresReiteration,
                            @Nullable Derivation derivation, boolean recordExplanations) {
                super(unifiedConceptMap, parent, resolver, root, requiresReiteration, derivation, recordExplanations);
                this.unifier = unifier;
                this.instanceRequirements = instanceRequirements;
                this.hash = Objects.hash(root, resolver, conceptMap, unifier, instanceRequirements, parent);
            }

            static Optional<Partial.Unified> unify(Partial<?> parent, Unifier unifier, Actor<ConclusionResolver> resolver,
                                                   Actor<? extends Resolver<?>> root, boolean recordExplanations) {
                Optional<Pair<ConceptMap, Instance>> unified = unifier.unify(parent.conceptMap());
                Derivation derivation = recordExplanations ? new AnswerState.Derivation(new HashMap<>()) : null;
                return unified.map(unification -> new Partial.Unified(
                        unification.first(), parent, unifier, unification.second(), resolver, root, false,
                        derivation, recordExplanations));

            }

            public Optional<Partial<?>> aggregateToUpstream(Map<Identifier.Variable, Concept> concepts) {
                Optional<ConceptMap> unUnified = unifier.unUnify(concepts, instanceRequirements);
                return unUnified.map(ans -> parent().with(new ConceptMap(ans.concepts()), true, resolvedBy(), this));
            }

            @Override
            public boolean isUnified() { return true; }

            @Override
            public Unified asUnified() { return this; }

            @Override
            Unified with(ConceptMap extension, boolean requiresReiteration, Actor<? extends Resolver<?>> extendedBy,
                         @Nullable Partial<?> extensionState) {
                Optional<Derivation> extendeDerivation = extendDerivation(extendedBy, extensionState);
                return new Unified(extendAnswer(extension), parent(), unifier, instanceRequirements, resolvedBy(), root(), requiresReiteration,
                                   extendeDerivation.orElse(null), recordExplanations());
            }

            public Unified extend(ConceptMap ans) {
                Map<Identifier.Variable.Retrievable, Concept> extended = new HashMap<>();
                extended.putAll(ans.concepts());
                extended.putAll(conceptMap.concepts());
                return new Unified(new ConceptMap(extended), parent(), unifier, instanceRequirements, resolvedBy(), root(),
                                   requiresReiteration, derivation(), recordExplanations());
            }

            @Override
            public String toString() {
                return "AnswerState.Partial.Unified{" +
                        "root=" + root() +
                        "resolver=" + resolvedBy() +
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

    public static class Derivation {
        public static final Derivation EMPTY = new Derivation(map());

        private Map<Actor<? extends Resolver<?>>, Partial<?>> answers;

        public Derivation(Map<Actor<? extends Resolver<?>>, Partial<?>> answers) {
            this.answers = map(answers);
        }

        public Derivation withAnswer(Actor<? extends Resolver<?>> resolver, Partial<?> answer) {
            Map<Actor<? extends Resolver<?>>, Partial<?>> copiedResolution = new HashMap<>(answers);
            copiedResolution.put(resolver, answer);
            return new Derivation(copiedResolution);
        }

        public void update(Map<Actor<? extends Resolver<?>>, Partial<?>> newResolutions) {
            assert answers.keySet().stream().noneMatch(key -> answers.containsKey(key)) :
                    "Cannot overwrite any derivations during an update";
            Map<Actor<? extends Resolver<?>>, Partial<?>> copiedResolutions = new HashMap<>(answers);
            copiedResolutions.putAll(newResolutions);
            this.answers = copiedResolutions;
        }

        public void replace(Map<Actor<? extends Resolver<?>>, Partial<?>> newResolutions) {
            this.answers = map(newResolutions);
        }

        public Map<Actor<? extends Resolver<?>>, Partial<?>> answers() {
            return this.answers;
        }

        @Override
        public String toString() {
            return "Derivation{" + "answers=" + answers + '}';
        }
    }
}
