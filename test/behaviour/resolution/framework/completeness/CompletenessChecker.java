package com.vaticle.typedb.core.test.behaviour.resolution.framework.completeness;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier;
import com.vaticle.typedb.core.logic.resolvable.Unifier.Requirements.Instance;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.Exceptions;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.reference.Reasoner;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.Utils.filterRetrievableVars;

public class CompletenessChecker {

    private final Reasoner referenceReasoner;
    private final RocksTransaction referenceTx;
    private final RocksSession session;

    public CompletenessChecker(Reasoner referenceReasoner, RocksTransaction referenceTx, RocksSession session) {
        this.referenceReasoner = referenceReasoner;
        this.session = session;
        this.referenceTx = referenceTx;
        assert !referenceTx.context().options().infer();
    }

    public static CompletenessChecker create(Reasoner referenceReasoner, RocksTransaction referenceTx,
                                             RocksSession session) {
        return new CompletenessChecker(referenceReasoner, referenceTx, session);
    }

    public void checkQuery(TypeQLMatch inferenceQuery) {
        // TODO: How do we handle disjunctions inside negations?
        Disjunction disjunction = Disjunction.create(inferenceQuery.conjunction().normalise());
        disjunction.conjunctions().forEach(conjunction -> {
            referenceTx.reasoner().executeTraversal(
                    new Disjunction(Collections.singletonList(conjunction)),
                    new Context.Query(referenceTx.context(), new Options.Query()),
                    filterRetrievableVars(conjunction.identifiers())
            ).forEachRemaining(answer -> {
                checkAnswer(answer, conjunction);
            });
        });
    }

    private void checkAnswer(ConceptMap answer, Conjunction conjunction) {
        Concludable.create(conjunction).forEach(concludable -> {
            Map<Rule, Set<Unifier>> applicableRules = concludable.applicableRules(referenceTx.concepts(),
                                                                                  referenceTx.logic());
            applicableRules.forEach(((rule, unifiers) -> unifiers.forEach(unifier -> {
                Optional<Pair<ConceptMap, Instance>> unified = unifier.unify(answer.filter(concludable.retrieves()));
                if (unified.isPresent()) {
                    ConceptMap conclusionAnswer = unified.get().first();
                    ConceptMap conditionAnswer = referenceReasoner.ruleRecorderMap().get(rule.getLabel())
                            .inferencesByConclusion().get(conclusionAnswer);
                    // Make sure that the unifier is valid for the particular answer we have
                    if (conditionAnswer != null) {
                        checkAnswer(conditionAnswer, rule.when());
                        checkConclusion(rule.conclusion(), conclusionAnswer);
                    }
                }
            })));
        });
    }

    private static boolean containsInferredConcept(ConceptMap answer) {
        for (Concept concept : answer.concepts().values()) {
            if (concept.isThing() && concept.asThing().isInferred()) return true;
        }
        return false;
    }

    private void checkConclusion(Rule.Conclusion conclusion, ConceptMap conclusionAnswer) {
        // TODO: Do we need to assert somewhere that there should only be one rule used to find a particular concludable answer across all of its explanations?



        // TODO: Realised this method should deal with a Conclusion not a Concludable. The unification is already done so we can work with the rule conclusion with its known structure.
        if (conclusion.isRelation()) {
            // TODO if bounds leave more than one variable unbound then this is unsupported
            iterate(conclusionAnswer.concepts()).filter((id, c) -> conclusion.generating().isPresent() && conclusion.generating().get().id().equals(id))
            conclusion.asRelation().relation().players()
        } else if (conclusion.isHas()) {
        } else if (conclusion.isVariableHas()) {
        } else if (conclusion.isExplicitHas()) {
        } else if (conclusion.isValue()) {
        } else if (conclusion.isIsa()) {

        }
        conclusion.

        Conjunction constrainedByIIDs = conclusion.pattern().clone();
        conclusionAnswer.concepts().forEach((identifier, concept) -> {
            if (concept.isThing()) {
                assert constrainedByIIDs.variable(identifier).isThing();
                // Make sure this isn't the generating variable, we want to leave that unconstrained.
                if (!(conclusion.generating().isPresent() && conclusion.generating().get().id().equals(identifier))) {
                    constrainedByIIDs.variable(identifier).asThing().iid(concept.asThing().getIID());
                }
            }
        });

        Disjunction concludableQuery = new Disjunction(Collections.singletonList(constrainedByIIDs));
        try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.READ,
                                                       new Options.Transaction().infer(true))) {
            int numAnswers = tx.reasoner().executeReasoner(concludableQuery,
                                                           conclusion.retrieves(),
                                                           new Context.Query(tx.context(), new Options.Query())).toList().size();
            if (numAnswers == 0) {
                throw new Exceptions.CompletenessException("Expected an answer which is not present.");
            } else if (numAnswers > 1) {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }
    }

}
