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
import grakn.core.traversal.common.Identifier;
import graql.lang.pattern.variable.Reference;

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
    protected final ConceptMap conceptMap;
    private final boolean recordExplanations;
    private final Derivation derivation;
    private final Actor<? extends Resolver<?>> resolver;
    final boolean requiresReiteration;

    AnswerState(ConceptMap conceptMap, Actor<? extends Resolver<?>> resolver, boolean requiresReiteration,
                @Nullable Derivation derivation, boolean recordExplanations) {
        this.conceptMap = conceptMap;
        this.resolver = resolver;
        this.requiresReiteration = requiresReiteration;
        this.derivation = derivation;
        this.recordExplanations = recordExplanations;
    }

    public boolean recordExplanations() {
        return recordExplanations;
    }

    public Actor<? extends Resolver<?>> resolver() {
        return resolver;
    }

    public ConceptMap conceptMap() {
        return conceptMap;
    }

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

    public static class Top extends AnswerState {

        private final Set<Reference.Name> filter;
        private final Derivation derivation;
        private final int hash;

        Top(ConceptMap conceptMap, @Nullable Set<Reference.Name> filter, Actor<? extends Resolver<?>> resolver, boolean recordExplanations,
            boolean requiresReiteration, @Nullable Derivation derivation) {
            super(conceptMap, resolver, requiresReiteration, new Derivation(map()), recordExplanations);
            this.filter = filter;
            this.derivation = derivation;
            this.hash = Objects.hash(conceptMap, filter);
        }

        public static Top initial(Set<Reference.Name> filter, boolean recordExplanations, Actor<? extends Resolver<?>> resolver) {
            return new Top(new ConceptMap(), filter, resolver, recordExplanations, false, null);
        }

        public Partial.Identity toDownstream() {
            return Partial.Identity.identity(conceptMap(), this, recordExplanations());
        }

        Top with(ConceptMap conceptMap, boolean requiresReiteration, @Nullable Derivation derivation) {
            return new Top(conceptMap, filter, resolver(), recordExplanations(), requiresReiteration, derivation);
        }

        @Override
        public String toString() {
            return "AnswerState.Top{" +
                    "conceptMap=" + conceptMap() +
                    "filter=" + filter +
                    '}';
        }

        @Override
        public ConceptMap conceptMap() {
            return super.conceptMap().filter(filter);
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
            return Objects.equals(conceptMap, top.conceptMap) &&
                    Objects.equals(filter, top.filter);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    public static abstract class Partial<Parent extends AnswerState> extends AnswerState {

        protected final Parent parent;
        private final Actor<? extends Resolver<?>> resolver;

        public Partial(ConceptMap partialAnswer, Parent parent, Actor<? extends Resolver<?>> resolver,
                       boolean requiresReiteration, @Nullable Derivation derivation, boolean recordExplanations) {
            super(partialAnswer, resolver, requiresReiteration, derivation, recordExplanations);
            this.parent = parent;
            this.resolver = resolver;
        }

        protected Parent parent() {
            return parent;
        }

        @Override
        public boolean requiresReiteration() {
            return requiresReiteration || parent().requiresReiteration();
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

        public Partial.Filtered filterToDownstream(Set<Reference.Name> filter) {
            return Filtered.filter(this, filter, recordExplanations());
        }

        public Partial.Mapped mapToDownstream(Mapping mapping) {
            return Mapped.map(this, mapping, recordExplanations());
        }

        public Optional<Partial.Unified> unifyToDownstream(Unifier unifier) {
            return Unified.unify(this, unifier, recordExplanations());
        }

        protected Optional<AnswerState.Derivation> extendedParentDerivation(Actor<? extends Resolver<?>> resolver) {
            if (recordExplanations()) return Optional.of(parent().derivation().withAnswer(resolver, this));
            return Optional.empty();
        }

        abstract Partial<?> with(ConceptMap conceptMap, Actor<? extends Resolver<?>> resolver,
                                 boolean requiresReiteration, @Nullable Derivation derivation);

        protected ConceptMap mergedWithParent(Map<Reference.Name, ? extends Concept> unmerged) {
            /*
            We MUST retain initial concepts, and add derived answers afterward. It's possible, and correct,
            that the derived answers overlap but are different: for example, when a subtype is found
            by the derived answer, but the initial already uses the supertype.
             */
            Map<Reference.Name, Concept> withInitial = new HashMap<>(unmerged);
            if (parent() != null) {
                // add the initial concept map second, to make sure we override and retain all of these
                withInitial.putAll(parent().conceptMap().concepts());
            }
            return new ConceptMap(withInitial);
        }

        public static class Identity extends Partial<Top> {

            private final int hash;

            private Identity(ConceptMap partialAnswer, Top parent, boolean requiresReiteration,
                             @Nullable Derivation derivation, boolean recordExplanations) {
                super(partialAnswer, parent, null, requiresReiteration, derivation, recordExplanations);
                this.hash = Objects.hash(conceptMap, parent);
            }

            static Identity identity(ConceptMap conceptMap, Top parent, boolean recordExplanations) {
                Derivation derivation = recordExplanations ? new AnswerState.Derivation(new HashMap<>()) : null;
                return new Identity(conceptMap, parent, false, derivation, recordExplanations);
            }

            @Override
            Partial<?> with(ConceptMap conceptMap, Actor<? extends Resolver<?>> resolver, boolean requiresReiteration,
                            @Nullable Derivation derivation) {
                return new Identity(conceptMap, parent(), requiresReiteration, derivation, recordExplanations());
            }

            public Top toTop() {
                return parent().asTop().with(conceptMap(), requiresReiteration(), derivation());
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
                return Objects.equals(conceptMap, identity.conceptMap) &&
                        Objects.equals(parent, identity.parent);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }

        public static class Filtered extends Partial<Partial<?>> {

            private final Set<Reference.Name> filter;
            private final int hash;

            private Filtered(ConceptMap filteredConceptMap, Partial<?> parent, Set<Reference.Name> filter,
                             Actor<? extends Resolver<?>> resolver, boolean requiresReiteration,
                             @Nullable Derivation derivation, boolean recordExplanations) {
                super(filteredConceptMap, parent, resolver, requiresReiteration, derivation, recordExplanations);
                this.filter = filter;
                this.hash = Objects.hash(conceptMap, parent, filter);
            }

            static Filtered filter(Partial<?> parent, Set<Reference.Name> filter, boolean recordExplanations) {
                Derivation derivation = recordExplanations ? new AnswerState.Derivation(new HashMap<>()) : null;
                return new Filtered(parent.conceptMap().filter(filter), parent, filter, null, false,
                                    derivation, recordExplanations);
            }

            public Partial<?> toUpstream(Actor<? extends Resolver<?>> resolver) {
                if (conceptMap().concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                return parent().with(mergedWithParent(conceptMap().filter(filter).concepts()), resolver, requiresReiteration(),
                                                 extendedParentDerivation(resolver).orElse(null));
            }

            @Override
            Filtered with(ConceptMap conceptMap, Actor<? extends Resolver<?>> resolver, boolean requiresReiteration,
                          @Nullable Derivation derivation) {
                return new Filtered(conceptMap, parent(), filter, resolver, requiresReiteration, derivation, recordExplanations());
            }

            @Override
            public boolean isFiltered() { return true; }

            @Override
            public Filtered asFiltered() { return this; }

            @Override
            public String toString() {
                return "AnswerState.Partial.Filtered{" +
                        "conceptMap=" + conceptMap() +
                        "filter=" + filter +
                        '}';
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Filtered filtered = (Filtered) o;
                return Objects.equals(conceptMap, filtered.conceptMap) &&
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
                           Actor<? extends Resolver<?>> resolver, boolean requiresReiteration,
                           @Nullable Derivation derivation, boolean recordExplanations) {
                super(mappedConceptMap, parent, resolver, requiresReiteration, derivation,
                      recordExplanations);
                this.mapping = mapping;
                this.hash = Objects.hash(conceptMap, parent, mapping);
            }

            static Mapped map(Partial<?> parent, Mapping mapping, boolean recordExplanations) {
                ConceptMap mappedConceptMap = mapping.transform(parent.conceptMap());
                Derivation derivation = recordExplanations ? new AnswerState.Derivation(new HashMap<>()) : null;
                return new Mapped(mappedConceptMap, parent, mapping, null, false, derivation,
                                  recordExplanations);
            }

            public Partial<?> aggregateToUpstream(ConceptMap additionalConcepts, Actor<? extends Resolver<?>> resolver) {
                return parent().with(mergedWithParent(mapping.unTransform(additionalConcepts).concepts()), resolver,
                                     requiresReiteration(), extendedParentDerivation(resolver).orElse(null));
            }

            public Partial<?> toUpstream(Actor<? extends Resolver<?>> resolver) {
                return parent().with(mergedWithParent(mapping.unTransform(this.conceptMap()).concepts()),resolver,
                                     requiresReiteration(), extendedParentDerivation(resolver).orElse(null));
            }

            @Override
            protected Mapped with(ConceptMap conceptMap, Actor<? extends Resolver<?>> resolver, boolean requiresReiteration,
                                  @Nullable Derivation derivation) {
                return new Mapped(conceptMap, parent(), mapping, resolver, requiresReiteration, derivation, recordExplanations());
            }

            @Override
            public boolean isMapped() { return true; }

            @Override
            public Mapped asMapped() { return this; }

            @Override
            public String toString() {
                return "AnswerState.Partial.Mapped{" +
                        "conceptMap=" + conceptMap() +
                        "mapping=" + mapping +
                        '}';
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Mapped mapped = (Mapped) o;
                return Objects.equals(conceptMap, mapped.conceptMap) &&
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

            private Unified(ConceptMap unifiedConceptMap, Partial<?> parent, Unifier unifier, Instance instanceRequirements,
                            Actor<? extends Resolver<?>> resolver, boolean requiresReiteration,
                            @Nullable Derivation derivation, boolean recordExplanations) {
                super(unifiedConceptMap, parent, resolver, requiresReiteration, derivation, recordExplanations);
                this.unifier = unifier;
                this.instanceRequirements = instanceRequirements;
                this.hash = Objects.hash(conceptMap, parent, unifier, instanceRequirements);
            }

            static Optional<Partial.Unified> unify(Partial<?> parent, Unifier unifier, boolean recordExplanations) {
                Optional<Pair<ConceptMap, Instance>> unified = unifier.unify(parent.conceptMap());
                Derivation derivation = recordExplanations ? new AnswerState.Derivation(new HashMap<>()) : null;
                return unified.map(unification -> new Partial.Unified(
                        unification.first(), parent, unifier, unification.second(), null, false,
                        derivation, recordExplanations));

            }

            public Optional<Partial<?>> aggregateToUpstream(Map<Identifier, Concept> identifiedConcepts,
                                                            Actor<? extends Resolver<?>> resolver) {
                Optional<ConceptMap> unUnified = unifier.unUnify(identifiedConcepts, instanceRequirements);
                return unUnified.map(ans -> parent().with(
                        mergedWithParent(new ConceptMap(ans.concepts()).concepts()), resolver, true,
                        extendedParentDerivation(resolver).orElse(null)));
            }

            @Override
            public boolean isUnified() { return true; }

            @Override
            public Unified asUnified() { return this; }

            @Override
            Unified with(ConceptMap conceptMap, Actor<? extends Resolver<?>> resolver, boolean requiresReiteration,
                         @Nullable Derivation derivation) {
                return new Unified(conceptMap, parent(), unifier, instanceRequirements, resolver, requiresReiteration,
                                   derivation, recordExplanations());
            }

            @Override
            public String toString() {
                return "AnswerState.Partial.Unified{" +
                        "conceptMap=" + conceptMap() +
                        "unifier=" + unifier +
                        "instanceRequirements=" + instanceRequirements +
                        '}';
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Unified unified = (Unified) o;
                return Objects.equals(conceptMap, unified.conceptMap) &&
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

        private Map<Actor<? extends Resolver<?>>, AnswerState> answers;

        public Derivation(Map<Actor<? extends Resolver<?>>, AnswerState> answers) {
            this.answers = map(answers);
        }

        public Derivation withAnswer(Actor<? extends Resolver<?>> resolver, AnswerState answer) {
            Map<Actor<? extends Resolver<?>>, AnswerState> copiedResolution = new HashMap<>(answers);
            copiedResolution.put(resolver, answer);
            return new Derivation(copiedResolution);
        }

        public void update(Map<Actor<? extends Resolver<?>>, AnswerState> newResolutions) {
            assert answers.keySet().stream().noneMatch(key -> answers.containsKey(key)) :
                    "Cannot overwrite any derivations during an update";
            Map<Actor<? extends Resolver<?>>, AnswerState> copiedResolutions = new HashMap<>(answers);
            copiedResolutions.putAll(newResolutions);
            this.answers = copiedResolutions;
        }

        public void replace(Map<Actor<? extends Resolver<?>>, AnswerState> newResolutions) {
            this.answers = map(newResolutions);
        }

        public Map<Actor<? extends Resolver<?>>, AnswerState> answers() {
            return this.answers;
        }

        @Override
        public String toString() {
            return "Derivation{" + "answers=" + answers + '}';
        }
    }
}
