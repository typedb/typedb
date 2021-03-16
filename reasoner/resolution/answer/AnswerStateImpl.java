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
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.traversal.common.Identifier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static grakn.core.common.iterator.Iterators.iterate;

public abstract class AnswerStateImpl implements AnswerState {

    private final Actor.Driver<? extends Resolver<?>> root;
    private final ConceptMap conceptMap;
    private final boolean requiresReiteration;

    AnswerStateImpl(ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
        this.conceptMap = conceptMap;
        this.root = root;
        this.requiresReiteration = requiresReiteration;
    }

    @Override
    public ConceptMap conceptMap() {
        return conceptMap;
    }

    @Override
    public boolean requiresReiteration() {
        return requiresReiteration;
    }

    @Override
    public Actor.Driver<? extends Resolver<?>> root() {
        return root;
    }


    // TODO clean up
    protected ConceptMap extendAnswer(ConceptMap extension) {
        return extendAnswer(extension, null);
    }

    // TODO clean up
    protected ConceptMap extendAnswer(ConceptMap extension, ExplainableAnswer explainableAnswer) {
            /*
            We MUST retain initial concepts, and add derived answers afterward. It's possible, and correct,
            that the derived answers overlap but are different: for example, when a subtype is found
            by the derived answer, but the initial already uses the supertype.
             */
        Map<Identifier.Variable.Retrievable, Concept> concepts = new HashMap<>(extension.concepts());
        // add the initial concept map second, to make sure we override and retain all of these
        concepts.putAll(conceptMap().concepts());
        return new ConceptMap(concepts, explainableAnswer);
    }


    public static abstract class TopImpl extends AnswerStateImpl implements Top {

        TopImpl(ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
            super(conceptMap, root, requiresReiteration);
        }

        public static abstract class MatchImpl extends TopImpl implements Match {

            private final Set<Identifier.Variable.Name> getFilter;
            private final boolean explainable;
            private final int hash;

            MatchImpl(Set<Identifier.Variable.Name> getFilter, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root,
                      boolean requiresReiteration, boolean explainable) {
                super(conceptMap.filter(getFilter), root, requiresReiteration);
                this.getFilter = getFilter;
                this.explainable = explainable;
                this.hash = Objects.hash(conceptMap, getFilter, root, requiresReiteration, explainable);
            }

            @Override
            public Set<Identifier.Variable.Name> getFilter() {
                return getFilter;
            }

            @Override
            public boolean explainable() {
                return explainable;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                AnswerStateImpl.TopImpl.MatchImpl that = (AnswerStateImpl.TopImpl.MatchImpl) o;
                return Objects.equals(root(), that.root()) &&
                        Objects.equals(conceptMap(), that.conceptMap()) &&
                        requiresReiteration() == that.requiresReiteration() &&
                        Objects.equals(getFilter, that.getFilter) &&
                        explainable == that.explainable;
            }

            @Override
            public int hashCode() {
                return hash;
            }

            public static class InitialImpl extends MatchImpl implements Initial {

                InitialImpl(Set<Identifier.Variable.Name> getFilter, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, boolean explainable) {
                    super(getFilter, conceptMap, root, requiresReiteration, explainable);
                }

                @Override
                public PartialImpl.CompoundImpl.MatchImpl.RootImpl toDownstream() {
                    //TODO we should use the original conceptmap instead of the filtered one
                    return new PartialImpl.CompoundImpl.MatchImpl.RootImpl(
                            explainable() ? new HashSet<>() : null, this, conceptMap(), root(), requiresReiteration(), explainable()
                    );
                }

                @Override
                public FinishedImpl finish(ConceptMap conceptMap, boolean requiresReiteration) {
                    return new FinishedImpl(getFilter(), conceptMap(), root(), requiresReiteration(), explainable());
                }

            }

            public static class FinishedImpl extends MatchImpl implements Finished {

                FinishedImpl(Set<Identifier.Variable.Name> getFilter, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, boolean explainable) {
                    super(getFilter, conceptMap, root, requiresReiteration, explainable);
                }

            }
        }

        public static abstract class ExplainImpl extends TopImpl implements Explain {

            private final int hash;

            ExplainImpl(ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                super(conceptMap, root, requiresReiteration);
                this.hash = Objects.hash(root, conceptMap, requiresReiteration);
            }

            public static class InitialImpl extends ExplainImpl implements Initial {

                private final int hash;

                InitialImpl(ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                    super(conceptMap, root, requiresReiteration);
                    this.hash = Objects.hash(root, conceptMap, requiresReiteration);
                }

                @Override
                public Partial.Compound.Explain.Root toDownstream() {
                    return new PartialImpl.CompoundImpl.ExplainImpl.RootImpl(null, this, conceptMap(), root(), requiresReiteration());
                }

                @Override
                public Finished finish(ConceptMap conceptMap, boolean requiresReiteration, Explanation explanation) {
                    return new FinishedImpl(explanation, extendAnswer(conceptMap), root(), requiresReiteration);
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    TopImpl.ExplainImpl.InitialImpl that = (TopImpl.ExplainImpl.InitialImpl) o;
                    return Objects.equals(root(), that.root()) &&
                            Objects.equals(conceptMap(), that.conceptMap()) &&
                            requiresReiteration() == that.requiresReiteration();
                }

                @Override
                public int hashCode() {
                    return hash;
                }

            }

            public static class FinishedImpl extends ExplainImpl implements Finished {

                private final Explanation explanation;
                private final int hash;

                FinishedImpl(Explanation explanation, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                    super(conceptMap, root, requiresReiteration);
                    assert explanation != null;
                    this.explanation = explanation;
                    this.hash = Objects.hash(root, conceptMap, requiresReiteration, explanation);
                }

                @Override
                public Explanation explanation() {
                    return explanation;
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    TopImpl.ExplainImpl.FinishedImpl that = (TopImpl.ExplainImpl.FinishedImpl) o;
                    return Objects.equals(root(), that.root()) &&
                            Objects.equals(conceptMap(), that.conceptMap()) &&
                            requiresReiteration() == that.requiresReiteration() &&
                            Objects.equals(explanation, that.explanation);
                }

                @Override
                public int hashCode() {
                    return hash;
                }
            }
        }
    }

    public static abstract class PartialImpl<SELF extends Partial<SELF, PARENT>, PARENT extends AnswerState>
            extends AnswerStateImpl implements Partial<SELF, PARENT> {

        private final PARENT parent;

        PartialImpl(PARENT parent, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
            super(conceptMap, root, requiresReiteration);
            this.parent = parent;
        }

        @Override
        public PARENT parent() {
            return parent;
        }

        public static abstract class CompoundImpl<SLF extends Compound<SLF, PRNT>, PRNT extends AnswerState>
                extends PartialImpl<SLF, PRNT> implements Compound<SLF, PRNT> {

            CompoundImpl(PRNT prnt, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                super(prnt, conceptMap, root, requiresReiteration);
            }

            public static abstract class MatchImpl<S extends Match<S, P>, P extends AnswerState> extends CompoundImpl<S, P>
                    implements Match<S, P> {

                private final boolean explainable;

                MatchImpl(P p, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, boolean explainable) {
                    super(p, conceptMap, root, requiresReiteration);
                    this.explainable = explainable;
                }

                @Override
                public boolean explainable() {
                    return explainable;
                }

                public static class RootImpl extends MatchImpl<Root, Top.Match.Initial> implements Root {

                    private final Set<ExplainableAnswer.Explainable> explainables;
                    private final int hash;

                    RootImpl(Set<ExplainableAnswer.Explainable> explainables, Top.Match.Initial parent, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root,
                             boolean requiresReiteration, boolean explainable) {
                        super(parent, conceptMap, root, requiresReiteration, explainable);
                        assert explainable ? explainables != null : explainables == null;
                        this.explainables = explainables;
                        this.hash = Objects.hash(root, conceptMap, parent, explainable); //note: NOT including explainables
                    }

                    @Override
                    public Root with(ConceptMap extension, boolean requiresReiteration) {
                        // TODO is this ever used.. should it?
                        return new RootImpl(explainables, parent(), extendAnswer(extension), root(), requiresReiteration, explainable());
                    }

                    @Override
                    public Root with(ConceptMap extension, boolean requiresReiteration, Conjunction source) {
                        Set<ExplainableAnswer.Explainable> explainablesExtended;
                        if (explainable()) {
                            explainablesExtended = new HashSet<>(explainables);
                            explainablesExtended.add(ExplainableAnswer.Explainable.unidentified(source));
                        } else {
                            explainablesExtended = null;
                        }
                        return new RootImpl(explainablesExtended, parent(), extendAnswer(extension), root(), requiresReiteration, explainable());
                    }

                    @Override
                    public Concludable.Match<Root> mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction) {
                        // TODO create a static constructor that hardcodes false
                        return new ConcludableImpl.MatchImpl<>(mapping, nextResolverConjunction, this, conceptMap(), root(), false, explainable());
                    }

                    @Override
                    public Top.Match.Finished toFinishedTop(Conjunction compoundConjunction) {
                        // TODO inject conjunction into the concept map here or in the parent constructor
                        return parent().finish(conceptMap(), requiresReiteration());
                    }
                }

                public static class NonRootImpl extends MatchImpl<NonRoot, Compound.Match<?, ?>> implements NonRoot {

                    private final Set<Identifier.Variable.Retrievable> filter;
                    private final int hash;

                    NonRootImpl(Set<Identifier.Variable.Retrievable> filter, Compound.Match<?, ?> parent,
                                ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration,
                                boolean explainable) {
                        // TODO we should apply filter here and not in the toUpstream() and aggregateToUpstream()
                        super(parent, conceptMap, root, requiresReiteration, explainable);
                        this.filter = filter;
                        this.hash = Objects.hash(root, conceptMap, parent, filter, explainable);
                    }

                    @Override
                    public Set<Identifier.Variable.Retrievable> filter() {
                        return filter;
                    }

                    @Override
                    public NonRoot with(ConceptMap extension, boolean requiresReiteration) {
                        return new NonRootImpl(filter(), parent(), extendAnswer(extension), root(), requiresReiteration, explainable());
                    }

                    @Override
                    public NonRoot with(ConceptMap extension, boolean requiresReiteration, Conjunction source) {
                        throw GraknException.of(ILLEGAL_STATE);
                    }

                    @Override
                    public Concludable.Match<NonRoot> mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction) {
                        // TODO create a static constructor that hardcodes false
                        return new ConcludableImpl.MatchImpl<>(mapping, nextResolverConjunction, this, conceptMap(), root(), false, explainable());
                    }

                    @Override
                    public Compound<?, ?> toUpstream() {
                        if (conceptMap().concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                        return parent().with(conceptMap().filter(filter), requiresReiteration() || parent().requiresReiteration());
                    }

                    @Override
                    public Compound<?, ?> aggregateToUpstream(ConceptMap conceptMap) {
                        if (conceptMap.concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                        return parent().with(conceptMap.filter(filter), requiresReiteration() || parent().requiresReiteration());
                    }

                }

                public static class ConditionImpl extends MatchImpl<Condition, Conclusion.Match> implements Condition {

                    private final Set<ExplainableAnswer.Explainable> explainables;
                    private final Set<Identifier.Variable.Retrievable> filter;

                    ConditionImpl(Set<Identifier.Variable.Retrievable> filter, Set<ExplainableAnswer.Explainable> explainables,
                                  Conclusion.Match parent, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root,
                                  boolean requiresReiteration, boolean explainable) {
                        super(parent, conceptMap, root, requiresReiteration, explainable);
                        this.filter = filter;
                        this.explainables = explainables;
                    }

                    @Override
                    public Condition with(ConceptMap extension, boolean requiresReiteration) {
                        return new ConditionImpl(filter, explainables, parent(), extendAnswer(extension), root(), requiresReiteration, explainable());
                    }

                    @Override
                    public Condition with(ConceptMap extension, boolean requiresReiteration, Conjunction source) {
                        // TODO we don't want to have explanation or source here at all, we only need them in `ExplainCondition`
                        Set<ExplainableAnswer.Explainable> explainablesClone = new HashSet<>(explainables);
                        explainablesClone.add(ExplainableAnswer.Explainable.unidentified(source));
                        return new ConditionImpl(filter, explainablesClone, parent(), extendAnswer(extension), root(), requiresReiteration, explainable());
                    }

                    @Override
                    public Concludable.Match<Condition> mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction) {
                        return new ConcludableImpl.MatchImpl<>(mapping, nextResolverConjunction, this, conceptMap(), root(), false, explainable());
                    }

                    @Override
                    public Conclusion.Match toUpstream() {
                        if (conceptMap().concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                        return parent().with(conceptMap().filter(filter), requiresReiteration() || parent().requiresReiteration());
                    }
                }
            }

            public static abstract class ExplainImpl<S extends Explain<S, P>, P extends AnswerState> extends CompoundImpl<S, P>
                    implements Explain<S, P> {

                ExplainImpl(P p, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                    super(p, conceptMap, root, requiresReiteration);
                }

                public static class RootImpl extends ExplainImpl<Root, Top.Explain.Initial> implements Explain.Root {

                    private final Explanation explanation;

                    RootImpl(Explanation explanation, Top.Explain.Initial parent, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                        super(parent, conceptMap, root, requiresReiteration);
                        this.explanation = explanation;
                    }

                    @Override
                    public Root with(ConceptMap extension, boolean requiresReiteration, Explanation explanation) {
                        assert explanation == null;
                        return new RootImpl(explanation, parent(), extendAnswer(extension), root(), requiresReiteration());
                    }

                    @Override
                    public boolean hasExplanation() {
                        // TODO we don't really want this, can we just not have this state before we get here?
                        return explanation != null;
                    }

                    @Override
                    public Concludable.Explain mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction) {
                        // TODO we don't really want the conjunction?
                        return new ConcludableImpl.ExplainImpl(null, mapping, this, conceptMap(), root(), requiresReiteration());
                    }

                    @Override
                    public Top.Explain.Finished toFinishedTop() {
                        assert explanation != null;
                        return new TopImpl.ExplainImpl.FinishedImpl(explanation, conceptMap(), root(), requiresReiteration());
                    }
                }

                // TODO this is new
                public static class ConditionImpl extends ExplainImpl<Condition, Conclusion.Explain> implements Condition {

                    private final Set<Identifier.Variable.Retrievable> filter;

                    ConditionImpl(Set<Identifier.Variable.Retrievable> filter, ConceptMap conceptMap, Conclusion.Explain parent, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                        super(parent, conceptMap, root, requiresReiteration);
                        this.filter = filter;
                    }


                    @Override
                    public Concludable<?, ?> mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction) {
                        return null;
                    }
                }
            }

        }

        public static abstract class ConcludableImpl<SLF extends Concludable<SLF, PRNT>, PRNT extends Compound<PRNT, ?>>
                extends PartialImpl<SLF, PRNT> implements Concludable<SLF, PRNT> {

            private final Mapping mapping;

            ConcludableImpl(Mapping mapping, PRNT parent, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                super(parent, conceptMap, root, requiresReiteration);
                this.mapping = mapping;
            }

            @Override
            public Mapping mapping() {
                return mapping;
            }

            public static class MatchImpl<P extends Compound.Match<P, ?>> extends ConcludableImpl<Match<P>, P> implements Match<P> {

                private final Conjunction conjunction;
                private boolean explainable;
                private final int hash;

                MatchImpl(Mapping mapping, Conjunction conjunction, P parent, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root,
                          boolean requiresReiteration, boolean explainable) {
                    super(mapping, parent, conceptMap, root, requiresReiteration);
                    this.conjunction = conjunction;
                    this.explainable = explainable;
                    this.hash = Objects.hash(root, conceptMap, mapping, parent, requiresReiteration, explainable);
                }

                @Override
                public boolean explainable() {
                    return explainable;
                }

                @Override
                public Match<P> with(ConceptMap extension, boolean requiresReiteration) {
                    return new MatchImpl<>(mapping(), conjunction, parent(), extendAnswer(extension), root(), requiresReiteration, explainable());
                }

                @Override
                public P toUpstreamInferred() {
                    boolean requiresReiteration = requiresReiteration() || parent().requiresReiteration();
                    return parent().with(mapping().unTransform(conceptMap()), requiresReiteration, conjunction);
                }

                @Override
                public P toUpstreamLookup(ConceptMap additionalConcepts, boolean isInferredConclusion) {
                    boolean requiresReiteration = requiresReiteration() || parent().requiresReiteration();
                    if (isInferredConclusion) {
                        return parent().with(mapping().unTransform(additionalConcepts), requiresReiteration, conjunction);
                    } else {
                        return parent().with(mapping().unTransform(additionalConcepts), requiresReiteration);
                    }
                }

                @Override
                public Optional<Conclusion.Match> toDownstream(Unifier unifier, Rule rule) {
                    return ConclusionImpl.MatchImpl.create(unifier, rule, this, root(), explainable());
                }

                @Override
                public Conjunction conjunction() {
                    // TODO do we want this to be public?
                    return conjunction;
                }
            }

            public static class ExplainImpl extends ConcludableImpl<Explain, Compound.Explain.Root> implements Explain {

                private final ConclusionAnswer conclusionAnswer;
                private final int hash;

                ExplainImpl(ConclusionAnswer conclusionAnswer, Mapping mapping, Compound.Explain.Root parent, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root,
                            boolean requiresReiteration) {
                    super(mapping, parent, conceptMap, root, requiresReiteration);
                    this.conclusionAnswer = conclusionAnswer;
                    this.hash = Objects.hash(root, conceptMap, mapping, parent, requiresReiteration, conclusionAnswer);
                }

                @Override
                public Explain with(ConceptMap extension, boolean requiresReiteration, ConclusionAnswer conclusionAnswer) {
                    assert conclusionAnswer == null;
                    return new ExplainImpl(conclusionAnswer, mapping(), parent(), extendAnswer(extension), root(), requiresReiteration);
                }

                @Override
                public Compound.Explain.Root toUpstreamInferred() {
                    boolean requiresReiteration = requiresReiteration() || parent().requiresReiteration();
                    Explanation explanation = new Explanation(conclusionAnswer.rule(), mapping(), conclusionAnswer, conclusionAnswer.conditionAnswer());
                    return parent().with(mapping().unTransform(this.conceptMap()), requiresReiteration, explanation);
                }

                @Override
                public Optional<Conclusion.Explain> toDownstream(Unifier unifier, Rule rule) {
                    return ConclusionImpl.ExplainImpl.create(unifier, rule, this, root());
                }

                @Override
                public ConclusionAnswer conclusionAnswer() {
                    // TODO do we want this to be public?
                    return conclusionAnswer;
                }
            }
        }

        public static abstract class ConclusionImpl<SLF extends Conclusion<SLF, PRNT>, PRNT extends Concludable<?, ?>>
                extends PartialImpl<SLF, PRNT> implements Conclusion<SLF, PRNT> {

            private final Rule rule;
            private final Unifier unifier;
            private final Unifier.Requirements.Instance instanceRequirements;

            ConclusionImpl(Rule rule, Unifier unifier, Unifier.Requirements.Instance instanceRequirements,
                           PRNT prnt, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                super(prnt, conceptMap, root, requiresReiteration);
                this.rule = rule;
                this.unifier = unifier;
                this.instanceRequirements = instanceRequirements;
            }

            @Override
            public Rule rule() {
                return rule;
            }

            @Override
            public Unifier unifier() {
                return unifier;
            }

            @Override
            public Unifier.Requirements.Instance instanceRequirements() {
                return instanceRequirements;
            }

            public static class MatchImpl extends ConclusionImpl<Match, Concludable.Match<?>> implements Match {

                private final boolean explainable;
                private final int hash;

                MatchImpl(Rule rule, Unifier unifier, Unifier.Requirements.Instance instanceRequirements, Concludable.Match<?> parent,
                          ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration,
                          boolean explainable) {
                    super(rule, unifier, instanceRequirements, parent, conceptMap, root, requiresReiteration);
                    this.explainable = explainable;
                    this.hash = Objects.hash(root, conceptMap, rule, unifier, instanceRequirements, parent, requiresReiteration, explainable);
                }

                static Optional<Match> create(Unifier unifier, Rule rule, Concludable.Match<?> parent, Actor.Driver<? extends Resolver<?>> root, boolean explainable) {
                    Optional<Pair<ConceptMap, Unifier.Requirements.Instance>> unified = unifier.unify(parent.conceptMap());
                    return unified.map(unification -> new MatchImpl(
                            rule, unifier, unification.second(), parent, unification.first(), root, false, explainable
                    ));
                }

                @Override
                public Match with(ConceptMap extension, boolean requiresReiteration) {
                    return new MatchImpl(rule(), unifier(), instanceRequirements(), parent(), extendAnswer(extension), root(), requiresReiteration, explainable());
                }

                @Override
                public Match extend(ConceptMap ans) {
                    return new MatchImpl(rule(), unifier(), instanceRequirements(), parent(), extendAnswer(ans), root(), requiresReiteration(), explainable());
                }

                @Override
                public Compound.Match.Condition toDownstream(Set<Identifier.Variable.Retrievable> filter) {
                    // TODO create static constructor to hide the `false`
                    return new CompoundImpl.MatchImpl.ConditionImpl(filter, explainable() ? new HashSet<>() : null, this, conceptMap(), root(), false, explainable());
                }

                @Override
                public Optional<Concludable.Match<?>> aggregateToUpstream(Map<Identifier.Variable, Concept> concepts) {
                    Optional<ConceptMap> unUnified = unifier().unUnify(concepts, instanceRequirements());
                    return unUnified.map(ans -> parent().with(ans, true));
                }

                @Override
                public boolean explainable() {
                    return explainable;
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    MatchImpl that = (MatchImpl) o;
                    return Objects.equals(root(), that.root()) &&
                            Objects.equals(conceptMap(), that.conceptMap()) &&
                            Objects.equals(parent(), that.parent()) &&
                            Objects.equals(rule(), that.rule()) &&
                            Objects.equals(unifier(), that.unifier()) &&
                            Objects.equals(instanceRequirements(), that.instanceRequirements()) &&
                            requiresReiteration() == that.requiresReiteration() &&
                            explainable == that.explainable;
                }

                @Override
                public int hashCode() {
                    return hash;
                }
            }

            public static class ExplainImpl extends ConclusionImpl<Explain, Concludable.Explain> implements Explain {

                private final ExplainableAnswer conditionAnswer;
                private final int hash;

                ExplainImpl(ExplainableAnswer conditionAnswer, Rule rule, Unifier unifier, Unifier.Requirements.Instance instanceRequirements, ConceptMap conceptMap, Concludable.Explain parent, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                    super(rule, unifier, instanceRequirements, parent, conceptMap, root, requiresReiteration);
                    this.conditionAnswer = conditionAnswer;
                    this.hash = Objects.hash(root, conceptMap, rule, unifier, instanceRequirements, parent, conditionAnswer);
                }

                static Optional<Explain> create(Unifier unifier, Rule rule, Concludable.Explain parent, Actor.Driver<? extends Resolver<?>> root) {
                    Optional<Pair<ConceptMap, Unifier.Requirements.Instance>> unified = unifier.unify(parent.conceptMap());
                    return unified.map(unification -> new ExplainImpl(
                            null, rule, unifier, unification.second(), unification.first(), parent, root, false
                    ));
                }

                @Override
                public Compound.Explain.Condition toDownstream(Set<Identifier.Variable.Retrievable> filter) {
                    return new CompoundImpl.ExplainImpl.ConditionImpl(filter, conceptMap(), this, root(), false);
                }

                @Override
                public Explain extend(ConceptMap ans) {
                    return new ExplainImpl(conditionAnswer, rule(), unifier(), instanceRequirements(), extendAnswer(ans), parent(), root(), requiresReiteration());
                }

                @Override
                public ExplainableAnswer conditionAnswer() {
                    // TODO do we want this to be public?
                    return conditionAnswer;
                }

                @Override
                public Explain with(ConceptMap extension, boolean requiresReiteration, ExplainableAnswer conditionAnswer) {
                    assert conditionAnswer == null;
                    return new ExplainImpl(conditionAnswer, rule(), unifier(), instanceRequirements(), extendAnswer(extension), parent(), root(), requiresReiteration);
                }

                @Override
                public Optional<Concludable.Explain> aggregateToUpstream(Map<Identifier.Variable, Concept> concepts) {
                    Optional<ConceptMap> unUnified = unifier().unUnify(concepts, instanceRequirements());
                    return unUnified.map(ans -> {
                        // TODO we could make this "free" (object creation only) and skip the toConceptMap if we just store the raw map
                        ConclusionAnswer conclusionAnswer = new ConclusionAnswer(rule(), toConceptMap(concepts), unifier(), conditionAnswer);
                        return parent().with(ans, true, conclusionAnswer);
                    });
                }


                private ConceptMap toConceptMap(Map<Identifier.Variable, Concept> concepts) {
                    Map<Identifier.Variable.Retrievable, Concept> filteredMap = new HashMap<>();
                    iterate(concepts.entrySet()).filter(entry -> entry.getKey().isRetrievable())
                            .forEachRemaining(entry -> filteredMap.put(entry.getKey().asRetrievable(), entry.getValue()));
                    return new ConceptMap(filteredMap);
                }

            }
        }

    }

}
