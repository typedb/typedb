package grakn.core.reasoner.resolution.answer;

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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static grakn.common.util.Objects.className;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;
import static grakn.core.common.exception.ErrorMessage.Pattern.INVALID_CASTING;

public interface AnswerStateApi {

    interface Explainable {

        boolean explainable();

    }

    ConceptMap conceptMap();

    boolean requiresReiteration();

    Actor.Driver<? extends Resolver<?>> root();

    default boolean isTop() { return false; }

    default Top asTop() { throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Top.class)); }

    default boolean isPartial() { return false; }

    default Partial<?, ?> asPartial() { throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Partial.class)); }

    interface Top extends AnswerStateApi {

        default boolean isTop() { return true; }

        default AnswerStateApi.Top asTop() { return this; }

        default boolean isMatch() { return false; }

        default Match asMatch() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), Match.class); }

        default boolean isExplain() { return false; }

        default Explain asExplain() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), Explain.class); }

        interface Match extends Top, Explainable {

            @Override
            default boolean isMatch() {
                return true;
            }

            @Override
            default Match asMatch() {
                return this;
            }

            default boolean isFinished() { return false; }

            default Finished asFinished() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), Finished.class); }

            interface Initial extends Match {

                Partial.Compound.Match.Root toDownstream();

                AnswerState.Top.Match.Finished finish(ConceptMap conceptMap, boolean requiresReiteration);

            }

            interface Finished extends Match {

                @Override
                default boolean isFinished() { return true; }

                @Override
                default Finished asFinished() { return this; }

            }

        }

        interface Explain extends Top {

            @Override
            default boolean isExplain() { return true; }

            @Override
            default Explain asExplain() { return this; }

            default boolean isFinished() { return false; }

            default Finished asFinished() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), Finished.class); }

            interface Initial extends Explain {

                Partial.Compound.Explain.Root toDownstream();

                AnswerState.Top.Explain.Finished finish(ConceptMap conceptMap, boolean requiresReiteration, Explanation explanation);

            }

            interface Finished extends Explain {

                Explanation explanation();

                @Override
                default boolean isFinished() { return true; }

                @Override
                default Finished asFinished() { return this; }

            }
        }
    }

    interface Partial<SELF extends Partial<SELF, PARENT>, PARENT extends AnswerStateApi> extends AnswerStateApi {

        PARENT parent();

        @Override
        default boolean isPartial() { return true; }

        @Override
        default Partial<?, ?> asPartial() { return this; }

        default boolean isCompound() { return false; }

        default boolean isConcludable() { return false; }

        default boolean isConclusion() { return false; }

        default Compound<?, ?> asCompound() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Compound.class));
        }

        default Concludable<?, ?> asConcludable() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Concludable.class));
        }

        default Conclusion<?, ?> asConclusion() {
            throw GraknException.of(INVALID_CASTING, className(this.getClass()), className(Conclusion.class));
        }

        interface Compound<SLF extends Compound<SLF, PRNT>, PRNT extends AnswerStateApi> extends Partial<SLF, PRNT> {

            AnswerStateApi.Partial.Concludable<?, ?> mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction);

            @Override
            default boolean isCompound() { return true; }

            @Override
            default Compound<?, ?> asCompound() { return this; }

            default boolean isMatch() { return false; }

            default Match<?, ?> asMatch() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), Match.class); }

            default boolean isExplain() { return false; }

            default Explain<?, ?> asExplain() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), Explain.class); }

            interface Match<S extends Match<S, P>, P extends AnswerStateApi> extends Compound<S, P>, Explainable {

                S with(ConceptMap extension, boolean requiresReiteration, Conjunction source);

                default boolean isRoot() { return false; }

                default Root asRoot() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), Root.class); }

                default boolean isCondition() { return false; }

                default Condition<?> asCondition() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), Condition.class); }

                default boolean isNonRoot() { return false; }

                default NonRoot asNonRoot() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), NonRoot.class); }

                interface Root extends Match<Root, Top.Match> {

                    @Override
                    Partial.Concludable.Match<Root> mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction);

                    AnswerState.Top.Match.Finished toFinishedTop(Conjunction conjunctionAnswered);

                    @Override
                    default boolean isRoot() { return true; }

                    @Override
                    default Root asRoot() { return this; }

                }

                interface NonRoot extends Match<NonRoot, Compound<?, ?>> {

                    Set<Identifier.Variable.Retrievable> filter();

                    @Override
                    Partial.Concludable.Match<NonRoot> mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction);

                    AnswerState.Partial.Compound<?, ?> toUpstream();

                    AnswerState.Partial.Compound<?, ?> aggregateToUpstream(ConceptMap conceptMap);

                    @Override
                    default boolean isNonRoot() { return true; }

                    @Override
                    default NonRoot asNonRoot() { return this; }

                }

                interface Condition<P extends Conclusion<P, ?>> extends Match<Condition<P>, P> {

                    @Override
                    Partial.Concludable.Match<Condition<P>> mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction);

                    P toUpstream(Conjunction conditionConjunction);

                    @Override
                    default boolean isCondition() {
                        return true;
                    }

                    @Override
                    default Condition<P> asCondition() { return this; }

                }
            }

            interface Explain<S extends Explain<S, P>, P extends AnswerStateApi> extends Compound<S, P> {

                default boolean isRoot() { return false; }

                default Root asRoot() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), Root.class); }

                default boolean isCondition() { return false; }

                default Condition asCondition() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), Condition.class); }

                interface Root extends Explain<Root, Top.Explain> {

                    Root with(ConceptMap extension, boolean requiresReiteration, Explanation explanation);

                    boolean hasExplanation();

                    @Override
                    Partial.Concludable.Explain mapToDownstream(Mapping mapping, Conjunction nextResolverConjunction);

                    Top.Explain.Finished toFinishedTop();

                    @Override
                    default boolean isRoot() { return true; }

                    @Override
                    default Root asRoot() { return this; }

                }

                interface Condition extends Explain<Condition, Conclusion.Explain> {

                    // TODO

                    @Override
                    default boolean isCondition() { return true; }

                    @Override
                    default Condition asCondition() { return this; }

                }

            }

        }

        interface Concludable<SLF extends Concludable<SLF, PRNT>, PRNT extends Compound<PRNT, ?>> extends Partial<SLF, PRNT> {

            PRNT toUpstreamInferred();

            Optional<? extends Conclusion<?, ?>> toDownstream(Unifier unifier, Rule rule);

            @Override
            default boolean isConcludable() { return true; }

            @Override
            default Concludable<?, ?> asConcludable() { return this; }

            default boolean isMatch() { return false; }

            default Match<?> asMatch() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), Match.class); }

            default boolean isExplain() { return false; }

            default Explain asExplain() { throw GraknException.of(ILLEGAL_CAST, this.getClass(), Explain.class); }

            interface Match<P extends Compound.Match<P, ?>> extends Concludable<Match<P>, P>, Explainable {

                @Override
                Optional<Conclusion.Match> toDownstream(Unifier unifier, Rule rule);

                P toUpstreamLookup(ConceptMap additionalConcepts, boolean isInferredConclusion);

                @Override
                default boolean isMatch() { return true; }

                @Override
                default Match<?> asMatch() { return this; }

            }

            interface Explain extends Concludable<Explain, Compound.Explain.Root> {

                @Override
                Optional<Partial.Conclusion.Explain> toDownstream(Unifier unifier, Rule rule);

                Explain with(ConceptMap extension, boolean requiresReiteration, ConclusionAnswer conclusionAnswer);

                @Override
                default boolean isExplain() {
                    return true;
                }

                @Override
                default Explain asExplain() { return this; }

            }

        }

        interface Conclusion<SLF extends AnswerStateApi.Partial.Conclusion<SLF, PRNT>, PRNT extends Concludable<?, ?>>
                extends AnswerStateApi.Partial<SLF, PRNT> {

            Rule rule();

            Unifier unifier();

            Unifier.Requirements.Instance instanceRequirements();

            Optional<? extends PRNT> aggregateToUpstream(Map<Identifier.Variable, Concept> concepts);

            Compound.Match.Condition<SLF> toDownstream(Set<Identifier.Variable.Retrievable> filter);

            SLF extend(ConceptMap ans);

            @Override
            default boolean isConclusion() { return true; }

            @Override
            default Partial.Conclusion<?, ?> asConclusion() { return this; }

            interface Match extends Conclusion<Match, Concludable.Match<?>>, Explainable {

                @Override
                Optional<Partial.Concludable.Match<?>> aggregateToUpstream(Map<Identifier.Variable, Concept> concepts);

            }

            interface Explain extends Conclusion<Explain, Concludable.Explain> {

                ExplainableAnswer conditionAnswer();

                Explain with(ConceptMap extension, boolean requiresReiteration, ExplainableAnswer conditionAnswer);

                @Override
                Optional<Partial.Concludable.Explain> aggregateToUpstream(Map<Identifier.Variable, Concept> concepts);

            }

        }

    }

}

