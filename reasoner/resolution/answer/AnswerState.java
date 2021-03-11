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
import grakn.core.concept.answer.ExplainableAnswer;
import grakn.core.concurrent.actor.Actor;
import grakn.core.logic.Rule;
import grakn.core.logic.resolvable.Unifier;
import grakn.core.logic.resolvable.Unifier.Requirements.Instance;
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.reasoner.resolution.resolver.RootResolver;
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

        public Top(ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
            super(conceptMap, root, requiresReiteration);
        }

        public boolean isTop() { return true; }

        public Top asTop() {
            return this;
        }

        public boolean isMatch() {
            return false;
        }

        public Match asMatch() {
            throw GraknException.of(ILLEGAL_CAST, this.getClass(), Match.class);
        }

        public boolean isExplain() {
            return false;
        }

        public Explain asExplain() {
            throw GraknException.of(ILLEGAL_CAST, this.getClass(), Explain.class);
        }

        public abstract static class Match extends Top {

            final Set<Identifier.Variable.Name> getFilter;

            Match(ConceptMap conceptMap, @Nullable Set<Identifier.Variable.Name> getFilter, Actor.Driver<? extends Resolver<?>> root,
                  boolean requiresReiteration) {
                super(conceptMap, root, requiresReiteration);
                this.getFilter = getFilter;
            }

            public static Initial initial(Set<Identifier.Variable.Name> getFilter, Actor.Driver<? extends Resolver<?>> root) {
                return new Initial(new ConceptMap(), getFilter, root);
            }

            @Override
            public boolean isMatch() {
                return true;
            }

            @Override
            public Match asMatch() {
                return this;
            }

            public boolean isFinished() {
                return false;
            }

            public Finished asFinished() {
                throw GraknException.of(ILLEGAL_CAST, this.getClass(), Finished.class);
            }

            @Override
            public ConceptMap conceptMap() {
                return conceptMap.filter(getFilter);
            }

            public static class Initial extends Match {

                private final int hash;

                Initial(ConceptMap conceptMap, @Nullable Set<Identifier.Variable.Name> getFilter, Actor.Driver<? extends Resolver<?>> root) {
                    super(conceptMap, getFilter, root, false);
                    this.hash = Objects.hash(conceptMap, getFilter, root);
                }

                public Partial.Compound.Root toDownstream() {
                    return Partial.Compound.Root.create(conceptMap(), this, root());
                }

                Finished finish(ConceptMap conceptMap, Conjunction conjunctionAnswered, Set<Conjunction> explainables,
                                boolean requiresReiteration) {
                    return new Finished(conceptMap, getFilter, root(), conjunctionAnswered, explainables, requiresReiteration);
                }

                @Override
                public String toString() {
                    return "AnswerState.Top.Match.Initial{" +
                            "root=" + root() +
                            ", conceptMap=" + conceptMap +
                            ", filter=" + getFilter +
                            '}';
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    Top.Match.Initial top = (Top.Match.Initial) o;
                    return Objects.equals(root(), top.root()) &&
                            Objects.equals(conceptMap, top.conceptMap) &&
                            Objects.equals(getFilter, top.getFilter);
                }

                @Override
                public int hashCode() {
                    return hash;
                }
            }

            public static class Finished extends Match {

                private final int hash;

                Finished(ConceptMap conceptMap, @Nullable Set<Identifier.Variable.Name> getFilter, Actor.Driver<? extends Resolver<?>> root,
                         Conjunction conjunctionAnswered, Set<Conjunction> explainables, boolean requiresReiteration) {
                    super(
                            new ConceptMap(
                                    conceptMap.concepts(),
                                    explainables.isEmpty() ? null : new ExplainableAnswer(conceptMap, conjunctionAnswered, explainables)
                            ),
                            getFilter, root, requiresReiteration
                    );
                    this.hash = Objects.hash(root, conceptMap, getFilter);
                }

                @Override
                public boolean isFinished() {
                    return true;
                }

                @Override
                public Finished asFinished() {
                    return this;
                }

                @Override
                public String toString() {
                    return "AnswerState.Top.Match.Finished{" +
                            "root=" + root() +
                            ", conceptMap=" + conceptMap +
                            ", filter=" + getFilter +
                            ", requiresReiteration=" + requiresReiteration +
                            '}';
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    Top.Match.Finished top = (Top.Match.Finished) o;
                    return Objects.equals(root(), top.root()) &&
                            Objects.equals(conceptMap, top.conceptMap) &&
                            Objects.equals(getFilter, top.getFilter) &&
                            requiresReiteration == top.requiresReiteration;
                }

                @Override
                public int hashCode() {
                    return hash;
                }
            }
        }

        public static class Explain extends Top {

            public Explain(ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                super(conceptMap, root, requiresReiteration);
            }

            public static Initial initial(ConceptMap bounds, Actor.Driver<RootResolver.Explain> root) {
                return new Initial(bounds, root, false);
            }

            @Override
            public ConceptMap conceptMap() {
                return conceptMap;
            }

            public boolean isFinished() {
                return false;
            }

            public Finished asFinished() {
                throw GraknException.of(ILLEGAL_CAST, this.getClass(), Explain.class);
            }

            public static class Initial extends Explain {

                public Initial(ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                    super(conceptMap, root, requiresReiteration);
                }

                public Partial.Compound.ExplainRoot toDownstream() {
                    return Partial.Compound.ExplainRoot.create(conceptMap, this, root());
                }

                public Finished finish(ConceptMap conceptMap, boolean requiresReiteration, Explanation explanation) {
                    return new Finished(conceptMap, root(), requiresReiteration, explanation);
                }
            }

            public static class Finished extends Explain {

                private final Explanation explanation;
                private final int hash;

                public Finished(ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration,
                                Explanation explanation) {
                    super(conceptMap, root, requiresReiteration);
                    this.explanation = explanation;
                    this.hash = Objects.hash(root, conceptMap, requiresReiteration, explanation);
                }

                @Override
                public boolean isFinished() {
                    return true;
                }

                @Override
                public Finished asFinished() {
                    return this;
                }

                public Explanation explanation() {
                    return explanation;
                }

                @Override
                public String toString() {
                    return "AnswerState.Top.Explain.Finished{" +
                            "root=" + root() +
                            ", conceptMap=" + conceptMap +
                            ", requiresReiteration=" + requiresReiteration +
                            ", explanation=" + explanation +
                            '}';
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    Top.Explain.Finished that = (Top.Explain.Finished) o;
                    return Objects.equals(root(), that.root()) &&
                            Objects.equals(conceptMap, that.conceptMap) &&
                            requiresReiteration == that.requiresReiteration &&
                            Objects.equals(explanation, that.explanation);
                }

                @Override
                public int hashCode() {
                    return hash;
                }

            }
        }
    }

    public static abstract class Partial<SELF extends Partial<SELF, PARENT>, PARENT extends AnswerState> extends AnswerState {

        protected final PARENT parent;

        public Partial(ConceptMap partialAnswer, PARENT parent, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
            super(partialAnswer, root, requiresReiteration);
            this.parent = parent;
        }


        public ConceptMap conceptMap() {
            return conceptMap;
        }

        protected PARENT parent() {
            return parent;
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

        public static abstract class Compound<PRNT extends AnswerState> extends Partial<Compound<PRNT>, PRNT> {


            public Compound(ConceptMap partialAnswer, PRNT parent, Actor.Driver<? extends Resolver<?>> root,
                            boolean requiresReiteration) {
                super(partialAnswer, parent, root, requiresReiteration);
            }

            public Compound.NonRoot filterToDownstream(Set<Identifier.Variable.Retrievable> filter) {
                return Compound.NonRoot.create(this, filter, root());
            }

            public abstract Concludable<?, ?> mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction);

            // TODO some types of Filtered states will consume Explanation, and some will throw away. Design smell?
            abstract Compound<PRNT> with(ConceptMap extension, boolean requiresReiteration, Conjunction source, Explanation explanation);

            @Override
            public boolean isCompound() { return true; }

            @Override
            public Compound<?> asCompound() { return this; }

            public boolean isRoot() { return false; }

            public Root asRoot() {
                throw GraknException.of(ILLEGAL_CAST, this.getClass(), Root.class);
            }

            public boolean isExplainRoot() { return false; }

            public ExplainRoot asExplainRoot() {
                throw GraknException.of(ILLEGAL_CAST, this.getClass(), Root.class);
            }

            public boolean isCondition() { return false; }

            public Condition asCondition() {
                throw GraknException.of(ILLEGAL_CAST, this.getClass(), Condition.class);
            }

            public boolean isNonRoot() { return false; }

            public NonRoot asNonRoot() {
                throw GraknException.of(ILLEGAL_CAST, this.getClass(), NonRoot.class);
            }

            public static class Root extends Compound<Top.Match.Initial> {

                private final Set<Conjunction> explainables;
                private final int hash;

                public Root(ConceptMap partialAnswer, Top.Match.Initial parent, Actor.Driver<? extends Resolver<?>> root,
                            boolean requiresReiteration, Set<Conjunction> explainables) {
                    super(partialAnswer, parent, root, requiresReiteration);
                    this.explainables = explainables;
                    this.hash = Objects.hash(root, conceptMap, parent);
                }

                static Root create(ConceptMap conceptMap, Top.Match.Initial parent, Actor.Driver<? extends Resolver<?>> root) {
                    return new Root(conceptMap, parent, root, false, new HashSet<>());
                }

                @Override
                public boolean isRoot() { return true; }

                @Override
                public Root asRoot() { return this; }

                @Override
                Root with(ConceptMap extension, boolean requiresReiteration) {
                    return new Root(extendAnswer(extension), parent(), root(), requiresReiteration, explainables);
                }

                @Override
                public Root with(ConceptMap extension, boolean requiresReiteration, Conjunction source, Explanation explanation) {
                    Set<Conjunction> explainablesClone = new HashSet<>(explainables);
                    explainablesClone.add(source);
                    return new Root(extendAnswer(extension), parent(), root(), requiresReiteration, explainablesClone);
                }

                @Override
                public Concludable.Match mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction) {
                    return Concludable.match(this, mapping, root(), nextResolverConjunction);
                }

                public Top.Match.Finished toFinishedTop(Conjunction conjunctionAnswered) {
                    return parent().finish(conceptMap(), conjunctionAnswered, explainables, requiresReiteration || parent().requiresReiteration());
                }

                @Override
                public String toString() {
                    return "AnswerState.Partial.Compound.Root{" +
                            "root=" + root() +
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
                            Objects.equals(conceptMap, root.conceptMap) &&
                            Objects.equals(parent, root.parent);
                }

                @Override
                public int hashCode() {
                    return hash;
                }

            }

            public static class ExplainRoot extends Compound<Top.Explain.Initial> {

                private final int hash;
                private final Explanation explanation;

                private ExplainRoot(ConceptMap partialAnswer, Top.Explain.Initial parent, Actor.Driver<? extends Resolver<?>> root,
                                   boolean requiresReiteration, @Nullable Explanation explanation) {
                    super(partialAnswer, parent, root, requiresReiteration);
                    this.explanation = explanation;
                    this.hash = Objects.hash(root, conceptMap, parent, requiresReiteration, explanation);
                }

                static ExplainRoot create(ConceptMap conceptMap, Top.Explain.Initial parent, Actor.Driver<? extends Resolver<?>> root) {
                    return new ExplainRoot(conceptMap, parent, root, false, null);
                }

                @Override
                public boolean isExplainRoot() {
                    return true;
                }

                @Override
                public ExplainRoot asExplainRoot() {
                    return this;
                }

                @Override
                ExplainRoot with(ConceptMap extension, boolean requiresReiteration) {
                    throw GraknException.of(ILLEGAL_STATE);
                }

                @Override
                public ExplainRoot with(ConceptMap extension, boolean requiresReiteration, Conjunction source, Explanation explanation) {
                    return new ExplainRoot(extendAnswer(extension), parent(), root(), requiresReiteration, explanation);
                }

                @Override
                public Concludable.Explain mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction) {
                    return Concludable.explain(this, mapping, root());
                }

                public Top.Explain.Finished toFinishedTop() {
                    return parent().finish(conceptMap(), requiresReiteration || parent().requiresReiteration(), explanation);
                }

                @Override
                public String toString() {
                    return "AnswerState.Partial.Compound.ExplainRoot{" +
                            "root=" + root() +
                            ", conceptMap=" + conceptMap() +
                            ", requiresReiteration=" + requiresReiteration +
                            ", explanation=" + explanation +
                            '}';
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    ExplainRoot that = (ExplainRoot) o;
                    return Objects.equals(root(), that.root()) &&
                            Objects.equals(conceptMap, that.conceptMap) &&
                            Objects.equals(parent, that.parent) &&
                            Objects.equals(requiresReiteration, that.requiresReiteration) &&
                            Objects.equals(explanation, that.explanation);
                }

                @Override
                public int hashCode() {
                    return hash;
                }

            }

            public static class Condition extends Compound<Conclusion> {

                private final Set<Conjunction> explainables;
                private final Set<Identifier.Variable.Retrievable> filter;
                private final int hash;

                private Condition(ConceptMap filteredMap, Conclusion parent, Set<Identifier.Variable.Retrievable> filter,
                                  Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, Set<Conjunction> explainables) {
                    super(filteredMap, parent, root, requiresReiteration);
                    this.explainables = explainables;
                    this.filter = filter;
                    this.hash = Objects.hash(root, conceptMap, parent, filter);
                }

                static Condition create(Conclusion parent, Set<Identifier.Variable.Retrievable> filter,
                                        Actor.Driver<? extends Resolver<?>> root) {
                    return new Condition(parent.conceptMap().filter(filter), parent, filter, root, false, set());
                }

                @Override
                public boolean isCondition() {
                    return true;
                }

                @Override
                public Condition asCondition() {
                    return this;
                }

                @Override
                public Concludable.Match mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction) {
                    return Concludable.match(this, mapping, root(), nextResolverConjunction);
                }

                public Conclusion toUpstream(Conjunction conditionConjunction) {
                    if (conceptMap().concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                    ExplainableAnswer conditionAnswer = explainables.isEmpty() ? null :
                            new ExplainableAnswer(conceptMap, conditionConjunction, explainables);
                    return parent().with(conceptMap().filter(filter), requiresReiteration || parent().requiresReiteration(), conditionAnswer);
                }

                @Override
                Condition with(ConceptMap extension, boolean requiresReiteration) {
                    return new Condition(extendAnswer(extension), parent(), filter, root(), requiresReiteration, explainables);
                }

                @Override
                public Condition with(ConceptMap extension, boolean requiresReiteration, Conjunction source, Explanation explanation) {
                    Set<Conjunction> explainablesClone = new HashSet<>(explainables);
                    explainablesClone.add(source);
                    return new Condition(extendAnswer(extension), parent(), filter, root(), requiresReiteration, explainablesClone);
                }

                @Override
                public String toString() {
                    return "AnswerState.Partial.Compound.Condition{" +
                            "root=" + root() +
                            ", conceptMap=" + conceptMap() +
                            ", filter=" + filter +
                            ", explainables=" + explainables +
                            '}';
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    Condition filtered = (Condition) o;
                    return Objects.equals(root(), filtered.root()) &&
                            Objects.equals(conceptMap, filtered.conceptMap) &&
                            Objects.equals(parent, filtered.parent) &&
                            Objects.equals(filter, filtered.filter);
                }

                @Override
                public int hashCode() {
                    return hash;
                }
            }

            public static class NonRoot extends Compound<Compound<?>> {

                private final Set<Identifier.Variable.Retrievable> filter;
                private final int hash;

                private NonRoot(ConceptMap filteredConceptMap, Compound<?> parent, Set<Identifier.Variable.Retrievable> filter,
                                Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                    super(filteredConceptMap, parent, root, requiresReiteration);
                    this.filter = filter;
                    this.hash = Objects.hash(root, conceptMap, parent, filter);
                }

                static NonRoot create(Compound<?> parent, Set<Identifier.Variable.Retrievable> filter,
                                      Actor.Driver<? extends Resolver<?>> root) {
                    return new NonRoot(parent.conceptMap().filter(filter), parent, filter, root, false);
                }

                @Override
                public boolean isNonRoot() { return true; }

                @Override
                public NonRoot asNonRoot() { return this; }

                @Override
                public Concludable.Match mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction) {
                    return Concludable.match(this, mapping, root(), nextResolverConjunction);
                }

                public Compound<?> toUpstream() {
                    if (conceptMap().concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                    return parent().with(conceptMap().filter(filter), requiresReiteration || parent().requiresReiteration());
                }

                public Compound<?> aggregateToUpstream(ConceptMap conceptMap) {
                    if (conceptMap.concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                    return parent().with(conceptMap.filter(filter), requiresReiteration || parent().requiresReiteration());
                }

                @Override
                NonRoot with(ConceptMap extension, boolean requiresReiteration) {
                    return new NonRoot(extendAnswer(extension), parent(), filter, root(), requiresReiteration);
                }

                @Override
                public NonRoot with(ConceptMap extension, boolean requiresReiteration, Conjunction source, Explanation explanation) {
                    return new NonRoot(extendAnswer(extension), parent(), filter, root(), requiresReiteration);
                }

                @Override
                public String toString() {
                    return "AnswerState.Compound.NonRoot{" +
                            "root=" + root() +
                            ", conceptMap=" + conceptMap() +
                            ", filter=" + filter +
                            '}';
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    NonRoot filtered = (NonRoot) o;
                    return Objects.equals(root(), filtered.root()) &&
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

        public static abstract class Concludable<SELF extends Concludable<SELF, PRNT>, PRNT extends AnswerState>
                extends Partial<SELF, PRNT> {

            final Mapping mapping;

            public Concludable(ConceptMap partialAnswer, Mapping mapping, PRNT prnt, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                super(partialAnswer, prnt, root, requiresReiteration);
                this.mapping = mapping;
            }

            static Match match(Compound<?> parent, Mapping mapping, Actor.Driver<? extends Resolver<?>> root, Conjunction sourceConjunction) {
                ConceptMap mappedConceptMap = mapping.transform(parent.conceptMap());
                return new Match(mappedConceptMap, parent, sourceConjunction, mapping, root, false, null);
            }

            static Explain explain(Compound.ExplainRoot parent, Mapping mapping, Actor.Driver<? extends Resolver<?>> root) {
                ConceptMap mappedConceptMap = mapping.transform(parent.conceptMap());
                return new Explain(mappedConceptMap, parent, mapping, root, false, null);
            }

            @Override
            public boolean isConcludable() { return true; }

            @Override
            public Concludable<?, ?> asConcludable() { return this; }

            public static class Match extends Concludable<Match, Compound<?>> {

                private final Conjunction sourceConjunction; //TODO having the parent conjunction in here is weird
                private final ConclusionAnswer conclusionAnswer;
                private final int hash;

                private Match(ConceptMap mappedConceptMap, Compound<?> parent, Conjunction sourceConjunction, Mapping mapping,
                                    Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, @Nullable ConclusionAnswer conclusionAnswer) {
                    super(mappedConceptMap, mapping, parent, root, requiresReiteration);
                    this.sourceConjunction = sourceConjunction;
                    this.conclusionAnswer = conclusionAnswer;
                    this.hash = Objects.hash(root, conceptMap, mapping, parent);
                }

                public Optional<Conclusion> unifyToDownstream(Unifier unifier, Rule rule) {
                    return Conclusion.unify(this, unifier, rule, root());
                }

                public Compound<?> aggregateToUpstream(ConceptMap additionalConcepts) {
                    return parent().with(mapping.unTransform(additionalConcepts), requiresReiteration || parent().requiresReiteration());
                }

                public Compound<?> toUpstream() {
                    // TODO should we carry the condition answer inside conclusion answer or outside?
                    // TODO compute variable mapping
                    Explanation explanation = new Explanation(conclusionAnswer.rule(), mapping, conclusionAnswer, conclusionAnswer.conditionAnswer());
                    return parent().with(mapping.unTransform(this.conceptMap()), requiresReiteration || parent().requiresReiteration(),
                                         sourceConjunction, explanation);
                }

                @Override
                Match with(ConceptMap extension, boolean requiresReiteration) {
                    // if we decide to keep an "explain" flag, we would use this endpoint
                    throw GraknException.of(ILLEGAL_STATE);
                }

                Match with(ConceptMap extension, boolean requiresReiteration, ConclusionAnswer conclusionAnswer) {
                    return new Match(extendAnswer(extension), parent(), sourceConjunction, mapping, root(), requiresReiteration, conclusionAnswer);
                }

                @Override
                public String toString() {
                    return "AnswerState.Partial.Concludable.Match{" +
                            "root=" + root() +
                            ", conceptMap=" + conceptMap() +
                            ", mapping=" + mapping +
                            '}';
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    Match mapped = (Match) o;
                    return Objects.equals(root(), mapped.root()) &&
                            Objects.equals(conceptMap, mapped.conceptMap) &&
                            Objects.equals(parent, mapped.parent) &&
                            Objects.equals(mapping, mapped.mapping);
                }

                @Override
                public int hashCode() {
                    return hash;
                }
            }

            public static class Explain extends Concludable<Explain, Compound.ExplainRoot> {

                private final ConclusionAnswer conclusionAnswer;
                private final int hash;

                private Explain(ConceptMap mappedConceptMap, Compound.ExplainRoot parent, Mapping mapping,
                              Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, @Nullable ConclusionAnswer conclusionAnswer) {
                    super(mappedConceptMap, mapping, parent, root, requiresReiteration);
                    this.conclusionAnswer = conclusionAnswer;
                    this.hash = Objects.hash(root, conceptMap, mapping, parent, conclusionAnswer);
                }

                public Optional<Conclusion> unifyToDownstream(Unifier unifier, Rule rule) {
                    return Conclusion.unify(this, unifier, rule, root());
                }

                public Compound.ExplainRoot aggregateToUpstream(ConceptMap additionalConcepts) {
                    return parent().with(mapping.unTransform(additionalConcepts), requiresReiteration || parent().requiresReiteration());
                }

                public Compound.ExplainRoot toUpstream() {
                    // TODO should we carry the condition answer inside conclusion answer or outside?
                    // TODO compute variable mapping
                    Explanation explanation = new Explanation(conclusionAnswer.rule(), mapping, conclusionAnswer, conclusionAnswer.conditionAnswer());
                    return parent().with(mapping.unTransform(this.conceptMap()), requiresReiteration || parent().requiresReiteration(),
                                         null, explanation);
                }

                @Override
                Explain with(ConceptMap extension, boolean requiresReiteration) {
                    // if we decide to keep an "explain" flag, we would use this endpoint
                    throw GraknException.of(ILLEGAL_STATE);
                }

                Explain with(ConceptMap extension, boolean requiresReiteration, ConclusionAnswer conclusionAnswer) {
                    return new Explain(extendAnswer(extension), parent(), mapping, root(), requiresReiteration, conclusionAnswer);
                }

                @Override
                public String toString() {
                    return "AnswerState.Partial.Concludable.Explain{" +
                            "root=" + root() +
                            ", conceptMap=" + conceptMap() +
                            ", mapping=" + mapping +
                            ", conclusionAnswer=" + conclusionAnswer +
                            '}';
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    Explain that = (Explain) o;
                    return Objects.equals(root(), that.root()) &&
                            Objects.equals(conceptMap, that.conceptMap) &&
                            Objects.equals(parent, that.parent) &&
                            Objects.equals(mapping, that.mapping) &&
                            Objects.equals(conclusionAnswer, that.conclusionAnswer);
                }

                @Override
                public int hashCode() {
                    return hash;
                }

            }


        }

        /*
        TODO we should create Conclusion.Initial and Conclusion.Final/Finished
         */
        public static class Conclusion extends Partial<Conclusion, Concludable> {

            private final Unifier unifier;
            private final Instance instanceRequirements;
            private final Rule rule;
            private final ExplainableAnswer conditionAnswer;

            private final int hash;

            private Conclusion(ConceptMap unifiedConceptMap, Concludable parent, Unifier unifier,
                               Instance instanceRequirements, Rule rule, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration,
                               @Nullable ExplainableAnswer conditionAnswer) {
                super(unifiedConceptMap, parent, root, requiresReiteration);
                this.unifier = unifier;
                this.instanceRequirements = instanceRequirements;
                this.rule = rule;
                this.conditionAnswer = conditionAnswer;
                this.hash = Objects.hash(root, conceptMap, rule, unifier, instanceRequirements, parent);
            }

            static Optional<Conclusion> unify(Concludable parent, Unifier unifier, Rule rule, Actor.Driver<? extends Resolver<?>> root) {
                Optional<Pair<ConceptMap, Instance>> unified = unifier.unify(parent.conceptMap());
                return unified.map(unification -> new Conclusion(
                        unification.first(), parent, unifier, unification.second(), rule, root, false, null
                ));
            }

            public Compound.Condition filterToDownstream(Set<Identifier.Variable.Retrievable> filter) {
                return Compound.Condition.create(this, filter, root());
            }

            public Optional<Concludable> aggregateToUpstream(Map<Identifier.Variable, Concept> concepts) {
                Optional<ConceptMap> unUnified = unifier.unUnify(concepts, instanceRequirements);
                return unUnified.map(ans -> {
                    // TODO we could make this "free" (object creation only) and skip the toConceptMap if we just store the raw map
                    ConclusionAnswer conclusionAnswer = new ConclusionAnswer(rule, toConceptMap(concepts), unifier, conditionAnswer);
                    return parent().with(ans, true, conclusionAnswer);
                });
            }

            private ConceptMap toConceptMap(Map<Identifier.Variable, Concept> concepts) {
                Map<Identifier.Variable.Retrievable, Concept> filteredMap = new HashMap<>();
                iterate(concepts.entrySet()).filter(entry -> entry.getKey().isRetrievable()).forEachRemaining(entry ->
                                                                                                                      filteredMap.put(entry.getKey().asRetrievable(), entry.getValue())
                );
                return new ConceptMap(filteredMap);
            }

            @Override
            public boolean isConclusion() { return true; }

            @Override
            public Conclusion asConclusion() { return this; }

            Conclusion with(ConceptMap extension, boolean requiresReiteration) {
                throw GraknException.of(ILLEGAL_STATE);
            }

            Conclusion with(ConceptMap extension, boolean requiresReiteration, ExplainableAnswer conditionAnswer) {
                return new Conclusion(extendAnswer(extension), parent(), unifier, instanceRequirements, rule, root(), requiresReiteration, conditionAnswer);
            }

            public Conclusion extend(ConceptMap ans) {
                Map<Identifier.Variable.Retrievable, Concept> extended = new HashMap<>();
                extended.putAll(ans.concepts());
                extended.putAll(conceptMap.concepts());
                return new Conclusion(new ConceptMap(extended), parent(), unifier, instanceRequirements, rule, root(),
                                      requiresReiteration, conditionAnswer);
            }

            @Override
            public String toString() {
                return "AnswerState.Partial.Conclusion{" +
                        "root=" + root() +
                        ", conceptMap=" + conceptMap() +
                        ", rule=" + rule.getLabel() +
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
                        Objects.equals(conceptMap, unified.conceptMap) &&
                        Objects.equals(parent, unified.parent) &&
                        Objects.equals(rule, unified.rule) &&
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
