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
import grakn.core.pattern.Conjunction;
import grakn.core.reasoner.resolution.framework.Resolver;
import grakn.core.traversal.common.Identifier;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static grakn.common.collection.Collections.set;
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

    protected ConceptMap extendAnswer(ConceptMap extension) {
        /*
        We MUST retain initial concepts, and add derived answers afterward. It's possible, and correct,
        that the derived answers overlap but are different: for example, when a subtype is found
        by the derived answer, but the initial already uses the supertype.
         */
        Map<Identifier.Variable.Retrievable, Concept> concepts = new HashMap<>(extension.concepts());
        // add the initial concept map second, to make sure we override and retain all of these
        concepts.putAll(conceptMap().concepts());
        return new ConceptMap(concepts, conceptMap().explainables().merge(extension.explainables()));
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
                super(conceptMap, root, requiresReiteration);
                this.getFilter = getFilter;
                this.explainable = explainable;
                this.hash = Objects.hash(root(), conceptMap(), requiresReiteration(), getFilter(), explainable());
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
                        Objects.equals(getFilter(), that.getFilter()) &&
                        explainable() == that.explainable();
            }

            @Override
            public int hashCode() {
                return hash;
            }

            public static class InitialImpl extends MatchImpl implements Initial {

                private InitialImpl(Set<Identifier.Variable.Name> getFilter, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, boolean explainable) {
                    super(getFilter, conceptMap, root, requiresReiteration, explainable);
                }

                public static InitialImpl create(Set<Identifier.Variable.Name> getFilter, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean explainable) {
                    return new InitialImpl(getFilter, conceptMap, root, false, explainable);
                }

                @Override
                public PartialImpl.CompoundImpl.RootImpl.MatchImpl toDownstream() {
                    return PartialImpl.CompoundImpl.RootImpl.MatchImpl.childOf(this);
                }

                @Override
                public FinishedImpl finish(ConceptMap conceptMap, boolean requiresReiteration) {
                    ConceptMap answer = conceptMap;
                    if (!explainable()) answer = conceptMap.filter(getFilter());
                    return FinishedImpl.create(getFilter(), answer, root(), requiresReiteration, explainable());
                }

            }

            public static class FinishedImpl extends MatchImpl implements Finished {

                private FinishedImpl(Set<Identifier.Variable.Name> getFilter, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, boolean explainable) {
                    super(getFilter, conceptMap, root, requiresReiteration, explainable);
                }

                public static FinishedImpl create(Set<Identifier.Variable.Name> getFilter, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, boolean explainable) {
                    ConceptMap initialAns = conceptMap;
                    if (!explainable) initialAns = conceptMap.filter(getFilter);
                    return new FinishedImpl(getFilter, initialAns, root, requiresReiteration, explainable);
                }
            }
        }

        public static abstract class ExplainImpl extends TopImpl implements Explain {

            ExplainImpl(ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                super(conceptMap, root, requiresReiteration);
            }

            public static class InitialImpl extends ExplainImpl implements Initial {

                private final int hash;

                public InitialImpl(ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                    super(conceptMap, root, requiresReiteration);
                    this.hash = Objects.hash(root(), conceptMap(), requiresReiteration());
                }

                @Override
                public Partial.Compound.Root.Explain toDownstream() {
                    return PartialImpl.CompoundImpl.RootImpl.ExplainImpl.childOf(this);
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
                    this.hash = Objects.hash(root(), conceptMap(), requiresReiteration(), explanation());
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
                            Objects.equals(explanation(), that.explanation());
                }

                @Override
                public int hashCode() {
                    return hash;
                }
            }
        }
    }

    public static abstract class PartialImpl<PARENT extends AnswerState>
            extends AnswerStateImpl implements Partial<PARENT> {

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
                extends PartialImpl<PRNT> implements Compound<SLF, PRNT> {

            CompoundImpl(PRNT prnt, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                super(prnt, conceptMap, root, requiresReiteration);
            }

            @Override
            public Retrievable<SLF> filterToRetrievable(Set<Identifier.Variable.Retrievable> filter) {
                return PartialImpl.PartialImpl.ConcludableImpl.RetrievableImpl.childOf(filter, getThis());
            }

            abstract SLF getThis();

            public static class NestableImpl extends CompoundImpl<Nestable, Compound<?, ?>> implements Nestable {

                private final Set<Identifier.Variable.Retrievable> filter;
                private final boolean explainable;
                private final int hash;

                NestableImpl(Set<Identifier.Variable.Retrievable> filter, Compound<?, ?> parent,
                             ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration,
                             boolean explainable) {
                    super(parent, conceptMap, root, requiresReiteration);
                    this.filter = filter;
                    this.explainable = explainable;
                    this.hash = Objects.hash(root(), parent(), conceptMap(), requiresReiteration(), filter());
                }

                static NestableImpl childOf(Set<Identifier.Variable.Retrievable> filter, Compound<?, ?> parent, boolean explainable) {
                    return new NestableImpl(filter, parent, parent.conceptMap().filter(filter), parent.root(), parent.requiresReiteration(), explainable);
                }

                @Override
                public Set<Identifier.Variable.Retrievable> filter() {
                    return filter;
                }

                @Override
                public Nestable with(ConceptMap extension, boolean requiresReiteration) {
                    return new NestableImpl(filter(), parent(), extendAnswer(extension), root(), requiresReiteration, explainable);
                }

                @Override
                public Concludable.Match<Nestable> toDownstream(Mapping mapping, grakn.core.logic.resolvable.Concludable concludable) {
                    return ConcludableImpl.MatchImpl.childOf(mapping, concludable, this, explainable);
                }

                @Override
                public Nestable filterToNestable(Set<Identifier.Variable.Retrievable> filter) {
                    return NestableImpl.childOf(filter, this, explainable);
                }

                @Override
                public Compound<?, ?> toUpstream() {
                    ConceptMap conceptMap = conceptMap();
                    if (!explainable) conceptMap = conceptMap.filter(filter);
                    return parent().with(conceptMap, requiresReiteration() || parent().requiresReiteration());
                }

                @Override
                Nestable getThis() {
                    return this;
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    NestableImpl that = (NestableImpl) o;
                    return Objects.equals(root(), that.root()) &&
                            Objects.equals(parent(), that.parent()) &&
                            Objects.equals(conceptMap(), that.conceptMap()) &&
                            requiresReiteration() == that.requiresReiteration() &&
                            Objects.equals(filter(), that.filter());
                }

                @Override
                public int hashCode() {
                    return hash;
                }
            }

            public static abstract class RootImpl<S extends Root<S, P>, P extends AnswerState> extends CompoundImpl<S, P> implements Root<S, P> {

                RootImpl(P parent, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                    super(parent, conceptMap, root, requiresReiteration);
                }

                public static class MatchImpl extends RootImpl<Match, Top.Match.Initial> implements Match {

                    private final boolean explainable;
                    private final int hash;

                    MatchImpl(Top.Match.Initial parent, ConceptMap conceptMap,
                              Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, boolean explainable) {
                        super(parent, conceptMap, root, requiresReiteration);
                        this.explainable = explainable;
                        this.hash = Objects.hash(root, parent, conceptMap, requiresReiteration(), explainable);
                    }

                    static MatchImpl childOf(Top.Match.Initial parent) {
                        return new MatchImpl(parent, parent.conceptMap(), parent.root(), parent.requiresReiteration(), parent.explainable());
                    }

                    @Override
                    public boolean explainable() {
                        return explainable;
                    }

                    @Override
                    public Match with(ConceptMap extension, boolean requiresReiteration) {
                        return new MatchImpl(parent(), extendAnswer(extension), root(), requiresReiteration, explainable());
                    }

                    @Override
                    public Concludable.Match<Match> toDownstream(Mapping mapping, grakn.core.logic.resolvable.Concludable concludable) {
                        return ConcludableImpl.MatchImpl.childOf(mapping, concludable, this, explainable());
                    }

                    @Override
                    public Nestable filterToNestable(Set<Identifier.Variable.Retrievable> filter) {
                        return NestableImpl.childOf(filter, this, explainable);
                    }

                    @Override
                    public Top.Match.Finished toFinishedTop(Conjunction compoundConjunction) {
                        return parent().finish(conceptMap(), requiresReiteration());
                    }

                    @Override
                    Match getThis() {
                        return this;
                    }

                    @Override
                    public boolean equals(Object o) {
                        if (this == o) return true;
                        if (o == null || getClass() != o.getClass()) return false;
                        MatchImpl that = (MatchImpl) o;
                        return Objects.equals(root(), that.root()) &&
                                Objects.equals(parent(), that.parent()) &&
                                Objects.equals(conceptMap(), that.conceptMap()) &&
                                requiresReiteration() == that.requiresReiteration() &&
                                explainable() == that.explainable();
                    }

                    @Override
                    public int hashCode() {
                        return hash;
                    }
                }

                public static class ExplainImpl extends RootImpl<Explain, Top.Explain.Initial> implements Explain {

                    private final Explanation explanation;
                    private final int hash;

                    private ExplainImpl(Explanation explanation, Top.Explain.Initial parent, ConceptMap conceptMap,
                                        Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                        super(parent, conceptMap, root, requiresReiteration);
                        this.explanation = explanation;
                        this.hash = Objects.hash(root(), parent(), conceptMap(), requiresReiteration(), explanation);
                    }

                    static ExplainImpl childOf(Top.Explain.Initial parent) {
                        return new ExplainImpl(null, parent, parent.conceptMap(), parent.root(), parent.requiresReiteration());
                    }

                    @Override
                    public Explain with(ConceptMap extension, boolean requiresReiteration) {
                        throw GraknException.of(ILLEGAL_STATE);
                    }

                    @Override
                    public Explain with(ConceptMap extension, boolean requiresReiteration, Explanation explanation) {
                        assert this.explanation == null;
                        return new ExplainImpl(explanation, parent(), extendAnswer(extension), root(), requiresReiteration());
                    }

                    @Override
                    public Concludable.Explain toDownstream(Mapping mapping, grakn.core.logic.resolvable.Concludable concludable) {
                        // note: we implement the method to conform to API, but do not use the conjunction when explaining
                        return ConcludableImpl.ExplainImpl.childOf(mapping, this);
                    }

                    @Override
                    public Nestable filterToNestable(Set<Identifier.Variable.Retrievable> filter) {
                        return NestableImpl.childOf(filter, this, false);
                    }

                    @Override
                    public Top.Explain.Finished toFinishedTop() {
                        assert explanation != null;
                        return parent().finish(conceptMap(), requiresReiteration(), explanation);
                    }

                    @Override
                    Explain getThis() {
                        return this;
                    }

                    @Override
                    public boolean equals(Object o) {
                        if (this == o) return true;
                        if (o == null || getClass() != o.getClass()) return false;
                        ExplainImpl that = (ExplainImpl) o;
                        return Objects.equals(root(), that.root()) &&
                                Objects.equals(parent(), that.parent()) &&
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


            public static abstract class ConditionImpl<S extends Condition<S, P>, P extends Conclusion<P, ?>> extends CompoundImpl<S, P>
                    implements Condition<S, P> {

                ConditionImpl(P parent, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                    super(parent, conceptMap, root, requiresReiteration);
                }

                @Override
                public Nestable filterToNestable(Set<Identifier.Variable.Retrievable> filter) {
                    return NestableImpl.childOf(filter, this, false);
                }

                public static class MatchImpl extends ConditionImpl<Match, Conclusion.Match> implements Match {

                    private final Set<Identifier.Variable.Retrievable> filter;
                    private final int hash;

                    private MatchImpl(Set<Identifier.Variable.Retrievable> filter, Conclusion.Match parent, ConceptMap conceptMap,
                                      Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                        super(parent, conceptMap, root, requiresReiteration);
                        this.filter = filter;
                        this.hash = Objects.hash(parent(), root(), conceptMap(), requiresReiteration(), filter);
                    }

                    static MatchImpl childOf(Set<Identifier.Variable.Retrievable> filter, Conclusion.Match parent) {
                        return new MatchImpl(filter, parent, parent.conceptMap().filter(filter), parent.root(), false);
                    }

                    @Override
                    public Match with(ConceptMap extension, boolean requiresReiteration) {
                        return new MatchImpl(filter, parent(), extendAnswer(extension), root(), requiresReiteration);
                    }

                    @Override
                    public Conclusion.Match toUpstream() {
                        return parent().with(conceptMap().filter(filter), requiresReiteration() || parent().requiresReiteration());
                    }

                    @Override
                    public Concludable.Match<Match> toDownstream(Mapping mapping, grakn.core.logic.resolvable.Concludable concludable) {
                        return ConcludableImpl.MatchImpl.childOf(mapping, concludable, this, false);
                    }

                    @Override
                    Match getThis() {
                        return this;
                    }

                    @Override
                    public boolean equals(Object o) {
                        if (this == o) return true;
                        if (o == null || getClass() != o.getClass()) return false;
                        MatchImpl that = (MatchImpl) o;
                        return Objects.equals(root(), that.root()) &&
                                Objects.equals(conceptMap(), that.conceptMap()) &&
                                Objects.equals(parent(), that.parent()) &&
                                requiresReiteration() == that.requiresReiteration() &&
                                Objects.equals(filter, that.filter);
                    }

                    @Override
                    public int hashCode() {
                        return hash;
                    }
                }

                public static class ExplainImpl extends ConditionImpl<Explain, Conclusion.Explain> implements Explain {

                    private final Set<Identifier.Variable.Retrievable> filter;
                    private final int hash;

                    private ExplainImpl(Set<Identifier.Variable.Retrievable> filter, Conclusion.Explain parent,
                                        ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root,
                                        boolean requiresReiteration) {
                        super(parent, conceptMap, root, requiresReiteration);
                        this.filter = filter;
                        this.hash = Objects.hash(root(), parent(), conceptMap(), requiresReiteration(), filter);
                    }

                    static ExplainImpl childOf(Set<Identifier.Variable.Retrievable> filter, Conclusion.Explain parent) {
                        return new ExplainImpl(filter, parent, parent.conceptMap().filter(filter), parent.root(), false);
                    }

                    @Override
                    public Explain with(ConceptMap extension, boolean requiresReiteration) {
                        return new ExplainImpl(filter, parent(), extendAnswer(extension), root(), requiresReiteration);
                    }

                    @Override
                    public Conclusion.Explain toUpstream() {
                        if (conceptMap().concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                        return parent().with(conceptMap(), requiresReiteration() || parent().requiresReiteration());
                    }

                    @Override
                    public Concludable.Match<Explain> toDownstream(Mapping mapping, grakn.core.logic.resolvable.Concludable concludable) {
                        return ConcludableImpl.MatchImpl.childOf(mapping, concludable, this, true); // record recursive explanations
                    }

                    @Override
                    Explain getThis() {
                        return this;
                    }

                    @Override
                    public boolean equals(Object o) {
                        if (this == o) return true;
                        if (o == null || getClass() != o.getClass()) return false;
                        ExplainImpl that = (ExplainImpl) o;
                        return Objects.equals(root(), that.root()) &&
                                Objects.equals(conceptMap(), that.conceptMap()) &&
                                Objects.equals(parent(), that.parent()) &&
                                requiresReiteration() == that.requiresReiteration() &&
                                Objects.equals(filter, that.filter);
                    }

                    @Override
                    public int hashCode() {
                        return hash;
                    }
                }
            }
        }

        public static abstract class ConcludableImpl<PRNT extends Compound<PRNT, ?>>
                extends PartialImpl<PRNT> implements Concludable<PRNT> {

            private final Mapping mapping;

            ConcludableImpl(Mapping mapping, PRNT parent, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                super(parent, conceptMap, root, requiresReiteration);
                this.mapping = mapping;
            }

            @Override
            public Mapping mapping() {
                return mapping;
            }

            public static class MatchImpl<P extends Compound<P, ?>> extends ConcludableImpl<P> implements Match<P> {

                private final grakn.core.logic.resolvable.Concludable concludable;
                private final boolean explainable;
                private final int hash;

                private MatchImpl(Mapping mapping, grakn.core.logic.resolvable.Concludable concludable, P parent, ConceptMap conceptMap,
                                  Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration, boolean explainable) {
                    super(mapping, parent, conceptMap, root, requiresReiteration);
                    this.concludable = concludable;
                    this.explainable = explainable;
                    this.hash = Objects.hash(root(), parent(), conceptMap(), requiresReiteration(), explainable(), mapping());
                }

                static <P extends Compound<P, ?>> MatchImpl<P> childOf(Mapping mapping, grakn.core.logic.resolvable.Concludable concludable,
                                                                       P parent, boolean explainable) {
                    return new MatchImpl<>(mapping, concludable, parent, mapping.transform(parent.conceptMap()),
                            parent.root(), parent.requiresReiteration(), explainable);
                }

                @Override
                public boolean explainable() {
                    return explainable;
                }

                @Override
                public Match<P> with(ConceptMap extension, boolean requiresReiteration) {
                    return new MatchImpl<>(mapping(), concludable, parent(), extendAnswer(extension), root(), requiresReiteration, explainable());
                }

                @Override
                public P toUpstreamInferred() {
                    ConceptMap upstreamAnswer = mapping().unTransform(conceptMap());
                    if (explainable()) upstreamAnswer = withExplainable(upstreamAnswer);
                    return parent().with(upstreamAnswer, requiresReiteration() || parent().requiresReiteration());
                }

                @Override
                public P toUpstreamLookup(ConceptMap additionalConcepts, boolean isInferredConclusion) {
                    boolean requiresReiteration = requiresReiteration() || parent().requiresReiteration();
                    ConceptMap upstreamAnswer = mapping().unTransform(additionalConcepts);
                    if (isInferredConclusion) {
                        if (explainable()) upstreamAnswer = withExplainable(upstreamAnswer);
                        return parent().with(upstreamAnswer, requiresReiteration);
                    } else {
                        return parent().with(upstreamAnswer, requiresReiteration);
                    }
                }

                private ConceptMap withExplainable(ConceptMap conceptMap) {
                    if (concludable.isRelation() || concludable.isAttribute() || concludable.isIsa()) {
                        return conceptMap.withExplainableConcept(concludable.generating().get().id(), concludable.pattern());
                    } else if (concludable.isHas()) {
                        return conceptMap.withExplainableAttrOwnership(concludable.asHas().owner().id(), concludable.asHas().attribute().id(), concludable.pattern());
                    } else {
                        throw GraknException.of(ILLEGAL_STATE);
                    }
                }

                @Override
                public Optional<Conclusion.Match> toDownstream(Unifier unifier, Rule rule) {
                    return ConclusionImpl.MatchImpl.childOf(unifier, rule, this);
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    MatchImpl that = (MatchImpl) o;
                    return Objects.equals(root(), that.root()) &&
                            Objects.equals(conceptMap(), that.conceptMap()) &&
                            Objects.equals(parent(), that.parent()) &&
                            requiresReiteration() == that.requiresReiteration() &&
                            explainable == that.explainable &&
                            Objects.equals(mapping(), that.mapping());
                }

                @Override
                public int hashCode() {
                    return hash;
                }
            }

            public static class ExplainImpl extends ConcludableImpl<Compound.Root.Explain> implements Explain {

                private final Explanation explanation;
                private final int hash;

                ExplainImpl(@Nullable Explanation explanation, Mapping mapping, Compound.Root.Explain parent, ConceptMap conceptMap,
                            Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                    super(mapping, parent, conceptMap, root, requiresReiteration);
                    this.explanation = explanation;
                    this.hash = Objects.hash(root(), parent(), conceptMap(), mapping(), requiresReiteration(), explanation);
                }

                static ExplainImpl childOf(Mapping mapping, Compound.Root.Explain parent) {
                    return new ExplainImpl(null, mapping, parent, mapping.transform(parent.conceptMap()), parent.root(), parent.requiresReiteration());
                }

                @Override
                public Explain with(ConceptMap extension, boolean requiresReiteration, Rule rule, ConceptMap conclusionAnser, Unifier unifier, ConceptMap conditionAnswer) {
                    assert this.explanation == null;
                    Explanation explanation = new Explanation(rule, transitiveMapping(mapping(), unifier), conclusionAnser, conditionAnswer);
                    return new ExplainImpl(explanation, mapping(), parent(), extendAnswer(extension), root(), requiresReiteration);
                }

                private Map<Identifier.Variable.Retrievable, Set<Identifier.Variable.Retrievable>> transitiveMapping(Mapping mapping, Unifier unifier) {
                    Map<Identifier.Variable.Retrievable, Set<Identifier.Variable.Retrievable>> merged = new HashMap<>();
                    mapping.mapping().forEach((from, to) -> {
                        Set<Identifier.Variable.Retrievable> tos = merged.computeIfAbsent(from, (key) -> new HashSet<>());
                        if (unifier.mapping().containsKey(to)) {
                            unifier.mapping().get(to).forEach(var -> {
                                if (var.isRetrievable()) tos.add(var.asRetrievable());
                            });
                        }
                    });
                    return merged;
                }

                @Override
                public Compound.Root.Explain toUpstreamInferred() {
                    boolean requiresReiteration = requiresReiteration() || parent().requiresReiteration();
                    return parent().with(mapping().unTransform(this.conceptMap()), requiresReiteration, explanation);
                }

                @Override
                public Optional<Conclusion.Explain> toDownstream(Unifier unifier, Rule rule) {
                    return ConclusionImpl.ExplainImpl.childOf(unifier, rule, this);
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    ExplainImpl that = (ExplainImpl) o;
                    return Objects.equals(root(), that.root()) &&
                            Objects.equals(parent(), that.parent()) &&
                            Objects.equals(conceptMap(), that.conceptMap()) &&
                            Objects.equals(mapping(), that.mapping()) &&
                            requiresReiteration() == that.requiresReiteration() &&
                            Objects.equals(explanation, that.explanation);
                }

                @Override
                public int hashCode() {
                    return hash;

                }
            }
        }

        public static abstract class ConclusionImpl<SLF extends Conclusion<SLF, PRNT>, PRNT extends Concludable<?>>
                extends PartialImpl<PRNT> implements Conclusion<SLF, PRNT> {

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

                private MatchImpl(Rule rule, Unifier unifier, Unifier.Requirements.Instance instanceRequirements, Concludable.Match<?> parent,
                                  ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration,
                                  boolean explainable) {
                    super(rule, unifier, instanceRequirements, parent, conceptMap, root, requiresReiteration);
                    this.explainable = explainable;
                    this.hash = Objects.hash(root(), parent(), conceptMap(), rule(), unifier(), instanceRequirements(), requiresReiteration(), explainable());
                }

                static Optional<Match> childOf(Unifier unifier, Rule rule, Concludable.Match<?> parent) {
                    Optional<Pair<ConceptMap, Unifier.Requirements.Instance>> unified = unifier.unify(parent.conceptMap());
                    return unified.map(unification -> new MatchImpl(
                            rule, unifier, unification.second(), parent, unification.first(), parent.root(), false, parent.explainable()
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
                public Compound.Root.Condition.Match toDownstream(Set<Identifier.Variable.Retrievable> filter) {
                    return CompoundImpl.ConditionImpl.MatchImpl.childOf(filter, this);
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
                            Objects.equals(parent(), that.parent()) &&
                            Objects.equals(conceptMap(), that.conceptMap()) &&
                            Objects.equals(rule(), that.rule()) &&
                            Objects.equals(unifier(), that.unifier()) &&
                            Objects.equals(instanceRequirements(), that.instanceRequirements()) &&
                            requiresReiteration() == that.requiresReiteration() &&
                            explainable() == that.explainable();
                }

                @Override
                public int hashCode() {
                    return hash;
                }
            }

            public static class ExplainImpl extends ConclusionImpl<Explain, Concludable.Explain> implements Explain {

                private final ConceptMap conditionAnswer;
                private final int hash;

                private ExplainImpl(ConceptMap conditionAnswer, Rule rule, Unifier unifier, Unifier.Requirements.Instance instanceRequirements,
                                    ConceptMap conceptMap, Concludable.Explain parent, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                    super(rule, unifier, instanceRequirements, parent, conceptMap, root, requiresReiteration);
                    this.conditionAnswer = conditionAnswer;
                    this.hash = Objects.hash(root(), parent(), conceptMap(), rule(), unifier(), instanceRequirements(), requiresReiteration(), conditionAnswer());
                }

                static Optional<Explain> childOf(Unifier unifier, Rule rule, Concludable.Explain parent) {
                    Optional<Pair<ConceptMap, Unifier.Requirements.Instance>> unified = unifier.unify(parent.conceptMap());
                    return unified.map(unification -> new ExplainImpl(
                            null, rule, unifier, unification.second(), unification.first(), parent, parent.root(), false
                    ));
                }

                @Override
                public Compound.Condition.Explain toDownstream(Set<Identifier.Variable.Retrievable> filter) {
                    return CompoundImpl.ConditionImpl.ExplainImpl.childOf(filter, this);
                }

                @Override
                public Explain extend(ConceptMap ans) {
                    return new ExplainImpl(conditionAnswer(), rule(), unifier(), instanceRequirements(), extendAnswer(ans), parent(), root(), requiresReiteration());
                }

                @Override
                public Explain with(ConceptMap conditionAnswer, boolean requiresReiteration) {
                    assert this.conditionAnswer() == null;
                    // note: we add more concepts to the conclusion answer than there are variables to preserve uniqueness of multiple explanations
                    return new ExplainImpl(conditionAnswer, rule(), unifier(), instanceRequirements(), extendAnswer(conditionAnswer), parent(), root(), requiresReiteration);
                }

                @Override
                public Optional<Concludable.Explain> aggregateToUpstream(Map<Identifier.Variable, Concept> concepts) {
                    Optional<ConceptMap> unUnified = unifier().unUnify(concepts, instanceRequirements());
                    return unUnified.map(ans -> {
                        ConclusionAnswer conclusionAnswer = new ConclusionAnswer(rule(), toConceptMap(concepts), unifier(), conditionAnswer());
                        return parent().with(ans, true, rule(), toConceptMap(concepts), unifier(), conditionAnswer());
                    });
                }

                private ConceptMap toConceptMap(Map<Identifier.Variable, Concept> concepts) {
                    Map<Identifier.Variable.Retrievable, Concept> filteredMap = new HashMap<>();
                    iterate(concepts.entrySet()).filter(entry -> entry.getKey().isRetrievable())
                            .forEachRemaining(entry -> filteredMap.put(entry.getKey().asRetrievable(), entry.getValue()));
                    return new ConceptMap(filteredMap);
                }

                @Override
                public ConceptMap conditionAnswer() {
                    return conditionAnswer;
                }

                @Override
                public boolean equals(Object o) {
                    if (this == o) return true;
                    if (o == null || getClass() != o.getClass()) return false;
                    ExplainImpl that = (ExplainImpl) o;
                    return Objects.equals(root(), that.root()) &&
                            Objects.equals(parent(), that.parent()) &&
                            Objects.equals(conceptMap(), that.conceptMap()) &&
                            Objects.equals(rule(), that.rule()) &&
                            Objects.equals(unifier(), that.unifier()) &&
                            Objects.equals(instanceRequirements(), that.instanceRequirements()) &&
                            requiresReiteration() == that.requiresReiteration() &&
                            Objects.equals(conditionAnswer(), that.conditionAnswer());
                }

                @Override
                public int hashCode() {
                    return hash;
                }


                static class ConclusionAnswer {

                    private final Rule rule;
                    private final ConceptMap conceptMap;
                    private final Unifier unifier;
                    private final ConceptMap conditionAnswer;
                    private final int hash;

                    public ConclusionAnswer(Rule rule, ConceptMap conceptMap, Unifier unifier, ConceptMap conditionAnswer) {
                        this.rule = rule;
                        this.conceptMap = conceptMap;
                        this.unifier = unifier;
                        this.conditionAnswer = conditionAnswer;
                        this.hash = Objects.hash(rule, conceptMap, unifier, conditionAnswer);
                    }

                    public Rule rule() {
                        return rule;
                    }

                    public ConceptMap conditionAnswer() {
                        return conditionAnswer;
                    }

                    public ConceptMap answer() {
                        return conceptMap;
                    }

                    @Override
                    public boolean equals(Object o) {
                        if (this == o) return true;
                        if (o == null || getClass() != o.getClass()) return false;
                        final ConclusionAnswer that = (ConclusionAnswer) o;
                        return Objects.equals(rule, that.rule) &&
                                Objects.equals(conceptMap, that.conceptMap) &&
                                Objects.equals(unifier, that.unifier) &&
                                Objects.equals(conditionAnswer, that.conditionAnswer);
                    }

                    @Override
                    public int hashCode() {
                        return hash;
                    }

                    public Unifier unifier() {
                        return unifier;
                    }
                }

            }
        }

        public static class RetrievableImpl<P extends Compound<P, ?>> extends PartialImpl<P> implements Retrievable<P> {

            private final Set<Identifier.Variable.Retrievable> filter;
            private final int hash;

            private RetrievableImpl(Set<Identifier.Variable.Retrievable> filter, P parent, ConceptMap conceptMap, Actor.Driver<? extends Resolver<?>> root, boolean requiresReiteration) {
                super(parent, conceptMap, root, requiresReiteration);
                this.filter = filter;
                this.hash = Objects.hash(root(), parent(), conceptMap(), requiresReiteration(), filter);
            }

            static <P extends Compound<P, ?>> RetrievableImpl<P> childOf(Set<Identifier.Variable.Retrievable> filter, P parent) {
                return new RetrievableImpl<>(filter, parent, parent.conceptMap().filter(filter), parent.root(), parent.requiresReiteration());
            }

            @Override
            public P aggregateToUpstream(ConceptMap concepts) {
                assert concepts.concepts().keySet().containsAll(conceptMap().concepts().keySet()) && filter.containsAll(concepts.concepts().keySet());
                if (concepts.concepts().isEmpty()) throw GraknException.of(ILLEGAL_STATE);
                return parent().with(concepts, parent().requiresReiteration());
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                RetrievableImpl that = (RetrievableImpl) o;
                return Objects.equals(root(), that.root()) &&
                        Objects.equals(parent(), that.parent()) &&
                        Objects.equals(conceptMap(), that.conceptMap()) &&
                        requiresReiteration() == that.requiresReiteration() &&
                        Objects.equals(filter, that.filter);
            }

            @Override
            public int hashCode() {
                return hash;
            }
        }
    }
}
