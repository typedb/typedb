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
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Unifier;
import grakn.core.logic.resolvable.Unifier.Requirements.Instance;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.resolver.ConclusionResolver;
import grakn.core.traversal.common.Identifier;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.set;
import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;
import static grakn.core.common.iterator.Iterators.iterate;

/*
TODO this class needs cleanup in terms of the Parent generic and introduction of a THIS generic, because we aren't using PARENT consistent for return types
TODO as well as aligning internal terminologies
 */
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

    public boolean isTop() { return false; }

    public Top asTop() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Top.class));
    }

    public boolean isPartial() { return false; }

    public Partial<?, ?> asPartial() {
        throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Partial.Compound.class));
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

            public Partial.Compound.Root toDownstream() {
                return Partial.Compound.Root.create(conceptMap(), this, root(), root());
            }

            Finished finish(ConceptMap conceptMap, grakn.core.pattern.Conjunction conjunctionAnswered, Set<grakn.core.pattern.Conjunction> explainables,
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

            private final ExplainableAnswer explainableAnswer;
            private final int hash;

            Finished(ConceptMap conceptMap, @Nullable Set<Identifier.Variable.Name> getFilter, Actor.Driver<? extends Resolver<?>> root,
                     grakn.core.pattern.Conjunction conjunctionAnswered, Set<grakn.core.pattern.Conjunction> explainables, boolean requiresReiteration) {
                super(conceptMap, getFilter, root, requiresReiteration);
                this.explainableAnswer = new ExplainableAnswer(conceptMap(), conjunctionAnswered, explainables);
                this.hash = Objects.hash(root, conceptMap, getFilter, explainableAnswer);
            }

            @Override
            public String toString() {
                return "AnswerState.Top.Finished{" +
                        "root=" + root() +
                        ", conceptMap=" + conceptMap +
                        ", filter=" + getFilter +
                        ", requiresReiteration=" + requiresReiteration +
                        ", explainableAnswer=" + explainableAnswer +
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
                        requiresReiteration == top.requiresReiteration &&
                        Objects.equals(explainableAnswer, top.explainableAnswer);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }
    }

    public static abstract class Partial<SELF extends Partial<SELF, PARENT>, PARENT extends AnswerState> extends AnswerState {

        protected final PARENT parent;
        private final Actor.Driver<? extends Resolver<?>> resolver; // resolver extending this answer state (eg the receiver)

        public Partial(ConceptMap partialAnswer, PARENT parent, Actor.Driver<? extends Resolver<?>> resolver,
                       Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
            super(partialAnswer, root, requiresReiteration);
            this.parent = parent;
            this.resolver = resolver;
        }


        public Concludable mapToDownstream(Mapping mapping, Actor.Driver<? extends Resolver<?>> nextResolver, grakn.core.pattern.Conjunction nextResolverConjunction) {
            assert this.isCompound();
            return Concludable.map(this.asCompound(), mapping, nextResolver, root(), nextResolverConjunction);
        }

        public ConceptMap conceptMap() {
            return conceptMap;
        }

        protected PARENT parent() {
            return parent;
        }

        public Actor.Driver<? extends Resolver<?>> resolvedBy() {
            return resolver;
        }

        @Override
        public boolean isPartial() { return true; }

        @Override
        public Partial<?, ?> asPartial() {
            return this;
        }

        public boolean isCompound() { return false; }

        public boolean isConcludable() { return false; }

        public boolean isConclusion() { return false; }

        public Compound<?> asCompound() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Compound.class));
        }

        public Concludable asConcludable() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Concludable.class));
        }

        public Conclusion asConclusion() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Conclusion.class));
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

        abstract SELF with(ConceptMap extension, boolean requiresReiteration);

        public Compound.NonRoot filterToDownstream(Set<Identifier.Variable.Retrievable> filter, Actor.Driver<? extends Resolver<?>> nextResolver) {
            return Compound.NonRoot.create(this, filter, nextResolver, root());
        }

        public static abstract class Compound<PRNT extends AnswerState> extends Partial<Compound<PRNT>, PRNT> {

            final Set<grakn.core.pattern.Conjunction> explainables;

            public Compound(ConceptMap partialAnswer, PRNT parent, Actor.Driver<? extends Resolver<?>> resolver,
                            Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, Set<grakn.core.pattern.Conjunction> explainables) {
                super(partialAnswer, parent, resolver, root, requiresReiteration);
                this.explainables = explainables;
            }

            // TODO some types of Filtered states will consume Explanation, and some will throw away. Design smell?
            abstract Compound<PRNT> with(ConceptMap extension, boolean requiresReiteration, grakn.core.pattern.Conjunction source, Explanation explanation);

            @Override
            public boolean isCompound() { return true; }

            @Override
            public Compound<?> asCompound() { return this; }

            public boolean isRoot() { return false; }

            public Root asRoot() {
                throw GraknException.of(ILLEGAL_CAST, this.getClass(), Root.class);
            }

            public boolean isSubset() { return false; }

            public NonRoot asSubset() {
                throw GraknException.of(ILLEGAL_CAST, this.getClass(), NonRoot.class);
            }

            public static class Root extends Compound<Top.Initial> {

                private final int hash;

                public Root(ConceptMap partialAnswer, Top.Initial parent, Actor.Driver<? extends Resolver<?>> resolver,
                            Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, Set<grakn.core.pattern.Conjunction> explainables) {
                    super(partialAnswer, parent, resolver, root, requiresReiteration, explainables);
                    this.hash = Objects.hash(root, resolver, conceptMap, parent);
                }

                static Root create(ConceptMap conceptMap, Top.Initial parent, Actor.Driver<? extends Resolver<?>> resolver,
                                   Actor.Driver<? extends Resolver<?>> root) {
                    return new Root(conceptMap, parent, resolver, root, false, new HashSet<>());
                }

                @Override
                public boolean isRoot() { return true; }

                @Override
                public Root asRoot() { return this; }

                @Override
                Root with(ConceptMap extension, boolean requiresReiteration) {
                    return new Root(extendAnswer(extension), parent(), resolvedBy(), root(), requiresReiteration, explainables);
                }

                @Override
                public Root with(ConceptMap extension, boolean requiresReiteration, grakn.core.pattern.Conjunction source, Explanation explanation) {
                    Set<grakn.core.pattern.Conjunction> explainablesClone = new HashSet<>(explainables);
                    explainablesClone.add(source);
                    return new Root(extendAnswer(extension), parent(), resolvedBy(), root(), requiresReiteration, explainablesClone);
                }

                public Top.Finished toFinishedTop(grakn.core.pattern.Conjunction conjunctionAnswered) {
                    return parent().finish(conceptMap(), conjunctionAnswered, explainables, requiresReiteration || parent().requiresReiteration());
                }

                @Override
                public String toString() {
                    return "AnswerState.Partial.Filtered{" +
                            "root=" + root() +
                            ", resolver=" + resolvedBy() +
                            ", conceptMap=" + conceptMap() +
                            ", explainables=" + explainables +
                            '}';
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    Root root = (Root) o;
                    return Objects.equals(root(), root.root()) &&
                            Objects.equals(resolvedBy(), root.resolvedBy()) &&
                            Objects.equals(conceptMap, root.conceptMap) &&
                            Objects.equals(parent, root.parent);
                }

                @Override
                public int hashCode() {
                    return hash;
                }

            }

            public static class NonRoot extends Compound<Partial<?, ?>> {

                private final Set<Identifier.Variable.Retrievable> filter;
                private final int hash;

                private NonRoot(ConceptMap filteredConceptMap, Partial<?, ?> parent, Set<Identifier.Variable.Retrievable> filter,
                                Actor.Driver<? extends Resolver<?>> resolver, Actor.Driver<? extends Resolver<?>> root,
                                boolean requiresReiteration, Set<grakn.core.pattern.Conjunction> explainables) {
                    super(filteredConceptMap, parent, resolver, root, requiresReiteration, explainables);
                    this.filter = filter;
                    this.hash = Objects.hash(root, resolver, conceptMap, parent, filter);
                }

                static NonRoot create(Partial<?, ?> parent, Set<Identifier.Variable.Retrievable> filter, Actor.Driver<? extends Resolver<?>> resolver,
                                      Actor.Driver<? extends Resolver<?>> root) {
                    return new NonRoot(parent.conceptMap().filter(filter), parent, filter, resolver, root, false, set());
                }

                @Override
                public boolean isSubset() { return true; }

                @Override
                public NonRoot asSubset() { return this; }

                public Partial<?, ?> toUpstream() {
                    if (conceptMap().concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                    return parent().with(conceptMap().filter(filter), requiresReiteration || parent().requiresReiteration());
                }

                public Partial<?, ?> aggregateToUpstream(ConceptMap conceptMap) {
                    if (conceptMap.concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                    return parent().with(conceptMap.filter(filter), requiresReiteration || parent().requiresReiteration());
                }

                @Override
                NonRoot with(ConceptMap extension, boolean requiresReiteration) {
                    return new NonRoot(extendAnswer(extension), parent(), filter, resolvedBy(), root(), requiresReiteration, explainables);
                }

                @Override
                public NonRoot with(ConceptMap extension, boolean requiresReiteration, grakn.core.pattern.Conjunction source, Explanation explanation) {
                    Set<grakn.core.pattern.Conjunction> explainablesClone = new HashSet<>(explainables);
                    explainablesClone.add(source);
                    return new NonRoot(extendAnswer(extension), parent(), filter, resolvedBy(), root(), requiresReiteration, explainablesClone);
                }

                @Override
                public String toString() {
                    return "AnswerState.Partial.Filtered{" +
                            "root=" + root() +
                            ", resolver=" + resolvedBy() +
                            ", conceptMap=" + conceptMap() +
                            ", filter=" + filter +
                            ", explainables=" + explainables +
                            '}';
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    NonRoot filtered = (NonRoot) o;
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
        }

        public static class Concludable extends Partial<Concludable, Compound<?>> {

            private final grakn.core.pattern.Conjunction sourceConjunction;
            private final Mapping mapping;
            private final ConclusionAnswer conclusionAnswer;
            private final int hash;

            private Concludable(ConceptMap mappedConceptMap, Compound<?> parent, grakn.core.pattern.Conjunction sourceConjunction, Mapping mapping,
                                Actor.Driver<? extends Resolver<?>> resolver, Actor.Driver<? extends Resolver<?>> root,
                                boolean requiresReiteration, @Nullable ConclusionAnswer conclusionAnswer) {
                super(mappedConceptMap, parent, resolver, root, requiresReiteration);
                this.sourceConjunction = sourceConjunction;
                this.mapping = mapping;
                this.conclusionAnswer = conclusionAnswer;
                this.hash = Objects.hash(root, resolver, conceptMap, mapping, parent);
            }

            static Concludable map(Compound<?> parent, Mapping mapping, Actor.Driver<? extends Resolver<?>> resolver,
                                   Actor.Driver<? extends Resolver<?>> root, grakn.core.pattern.Conjunction sourceConjunction) {
                ConceptMap mappedConceptMap = mapping.transform(parent.conceptMap());
                return new Concludable(mappedConceptMap, parent, sourceConjunction, mapping, resolver, root, false, null);
            }

            public Optional<Conclusion> unifyToDownstream(Unifier unifier, Rule rule, Actor.Driver<ConclusionResolver> nextResolver) {
                return Conclusion.unify(this, unifier, rule, nextResolver, root());
            }

            public Compound<?> aggregateToUpstream(ConceptMap additionalConcepts) {
                return parent().with(mapping.unTransform(additionalConcepts), requiresReiteration || parent().requiresReiteration());
            }

            public Compound<?> toUpstream() {
                // TODO should we carry the condition answer inside conclusion answer or outside?
                Explanation explanation = new Explanation(conclusionAnswer.rule().getLabel(), null, conclusionAnswer, conclusionAnswer.conditionAnswer());
                return parent().with(mapping.unTransform(this.conceptMap()), requiresReiteration || parent().requiresReiteration(),
                                     sourceConjunction, explanation);
            }

            @Override
            Concludable with(ConceptMap extension, boolean requiresReiteration) {
                // if we decide to keep an "explain" flag, we would use this endpoint
                throw GraknException.of(ILLEGAL_STATE);
            }

            Concludable with(ConceptMap extension, boolean requiresReiteration, ConclusionAnswer conclusionAnswer) {
                return new Concludable(extendAnswer(extension), parent(), sourceConjunction, mapping, resolvedBy(), root(), requiresReiteration, conclusionAnswer);

            }

            @Override
            public boolean isConcludable() { return true; }

            @Override
            public Concludable asConcludable() { return this; }

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
                Concludable mapped = (Concludable) o;
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

        public static class Conclusion extends Partial<Conclusion, Concludable> {

            private final Unifier unifier;
            private final Instance instanceRequirements;
            private final Rule rule;
            private final int hash;

            private Conclusion(ConceptMap unifiedConceptMap, Concludable parent, Unifier unifier,
                               Instance instanceRequirements, Rule rule, Actor.Driver<? extends Resolver<?>> resolver,
                               Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                super(unifiedConceptMap, parent, resolver, root, requiresReiteration);
                this.unifier = unifier;
                this.instanceRequirements = instanceRequirements;
                this.rule = rule;
                this.hash = Objects.hash(root, resolver, conceptMap, unifier, instanceRequirements, parent);
            }

            static Optional<Conclusion> unify(Concludable parent, Unifier unifier, Rule rule, Actor.Driver<ConclusionResolver> resolver,
                                              Actor.Driver<? extends Resolver<?>> root) {
                Optional<Pair<ConceptMap, Instance>> unified = unifier.unify(parent.conceptMap());
                return unified.map(unification -> new Conclusion(
                        unification.first(), parent, unifier, unification.second(), rule, resolver, root, false
                ));
            }

            public Optional<Concludable> aggregateToUpstream(Map<Identifier.Variable, Concept> concepts) {
                Optional<ConceptMap> unUnified = unifier.unUnify(concepts, instanceRequirements);
                return unUnified.map(ans -> {
                    // TODO we could make this "free" (object creation only) and skip the toConceptMap if we just store the raw map
                    ConclusionAnswer conclusionAnswer = new ConclusionAnswer(rule, toConceptMap(concepts), unifier);
                    return parent().with(ans, true, conclusionAnswer);
                });
            }

            private ConceptMap toConceptMap(Map<Identifier.Variable, Concept> concepts) {
                Map<Identifier.Variable.Retrievable, Concept> filteredMap = new HashMap<>();
                iterate(concepts.entrySet()).filter(entry -> entry.getKey().isRetrievable()).forEachRemaining(entry -> {
                    filteredMap.put(entry.getKey().asRetrievable(), entry.getValue());
                });
                return new ConceptMap(filteredMap);
            }

            @Override
            public boolean isConclusion() { return true; }

            @Override
            public Conclusion asConclusion() { return this; }

            Conclusion with(ConceptMap extension, boolean requiresReiteration) {
                return new Conclusion(extendAnswer(extension), parent(), unifier, instanceRequirements, rule, resolvedBy(), root(), requiresReiteration);
            }

            public Conclusion extend(ConceptMap ans) {
                Map<Identifier.Variable.Retrievable, Concept> extended = new HashMap<>();
                extended.putAll(ans.concepts());
                extended.putAll(conceptMap.concepts());
                return new Conclusion(new ConceptMap(extended), parent(), unifier, instanceRequirements, rule, resolvedBy(), root(),
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
                Conclusion unified = (Conclusion) o;
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
