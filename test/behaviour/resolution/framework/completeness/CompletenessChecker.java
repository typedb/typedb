package com.vaticle.typedb.core.test.behaviour.resolution.framework.completeness;

import com.vaticle.typedb.common.collection.Pair;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Context;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.logic.Rule;
import com.vaticle.typedb.core.logic.resolvable.Concludable;
import com.vaticle.typedb.core.logic.resolvable.Unifier.Requirements.Instance;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.rocks.RocksSession;
import com.vaticle.typedb.core.rocks.RocksTransaction;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.common.Exceptions;
import com.vaticle.typedb.core.test.behaviour.resolution.framework.reference.Reasoner;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.util.Collections;
import java.util.HashSet;
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
                checkConjunctionConcludables(answer, conjunction);
            });
        });
    }

    private void checkConjunctionConcludables(ConceptMap answer, Conjunction conjunction) {
        Concludable.create(conjunction).forEach(concludable -> {
            concludable
                    .applicableRules(referenceTx.concepts(), referenceTx.logic())
                    .forEach(((rule, unifiers) -> unifiers.forEach(unifier -> {
                        Optional<Pair<ConceptMap, Instance>> unified =
                                unifier.unify(answer.filter(concludable.retrieves()));
                        if (unified.isPresent()) {
                            ConceptMap conclusionAnswer = unified.get().first();
                            ConceptMap conditionAnswer = referenceReasoner.ruleRecorderMap().get(rule.getLabel())
                                    .inferencesByConclusion().get(conclusionAnswer);
                            // Make sure that the unifier is valid for the particular answer we have
                            if (conditionAnswer != null) {
                                checkConjunctionConcludables(conditionAnswer, rule.when());
                                checkConclusion(rule.conclusion(), conclusionAnswer);
                            }
                        }
                    })));
        });
    }

    private void checkConclusion(Rule.Conclusion conclusion, ConceptMap conclusionAnswer) {
        // TODO: Do we need to assert somewhere that there should only be one rule used to find a particular
        //  concludable answer across all of its explanations?
        ConceptMap conclusionBounds = removeGeneratingBound(conclusion, conclusionAnswer);
        validateFullyBound(conclusion, conclusionBounds);
        Conjunction conclusionWithIIDs = constrainByIIDs(conclusion, conclusionAnswer);
        validateInference(conclusion, conclusionWithIIDs);
    }

    private void validateInference(Rule.Conclusion conclusion, Conjunction conclusionWithIIDs) {
        // Validate that this conclusion can be reached using the production reasoner
        Disjunction concludableQuery = new Disjunction(Collections.singletonList(conclusionWithIIDs));
        try (RocksTransaction tx = session.transaction(Arguments.Transaction.Type.READ,
                                                       new Options.Transaction().infer(true))) {
            int numAnswers = tx.reasoner().executeReasoner(concludableQuery,
                                                           conclusion.retrievableIds(),
                                                           new Context.Query(tx.context(), new Options.Query())).toList().size();
            if (numAnswers == 0) {
                throw new Exceptions.CompletenessException("Expected an answer which is not present.");
            } else if (numAnswers > 1) {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }
    }

    private static Conjunction constrainByIIDs(Rule.Conclusion conclusion, ConceptMap conclusionBounds) {
        Conjunction constrainedByIIDs = conclusion.conjunction().clone();
        conclusionBounds.concepts().forEach((identifier, concept) -> {
            if (concept.isThing()) {
                assert constrainedByIIDs.variable(identifier).isThing();
                // Make sure this isn't the generating variable, we want to leave that unconstrained.
                constrainedByIIDs.variable(identifier).asThing().iid(concept.asThing().getIID());
            }
        });
        return constrainedByIIDs;
    }

    private static ConceptMap removeGeneratingBound(Rule.Conclusion conclusion, ConceptMap conclusionBounds) {
        // Remove the bound for the variable that the conclusion may generate
        Set<Identifier.Variable.Retrievable> nonGenerating = new HashSet<>(conclusion.retrievableIds());
        if (conclusion.generating().isPresent()) nonGenerating.remove(conclusion.generating().get().id());
        return conclusionBounds.filter(nonGenerating);
    }

    private static void validateFullyBound(Rule.Conclusion conclusion, ConceptMap conclusionAnswer) {
        // If any variable is unbound then throw
        iterate(conclusion.retrievableIds()).forEachRemaining(v -> {
            if (conclusionAnswer.concepts().get(v) == null) {
                throw new UnsupportedOperationException("Completion testing does not yet support non-generated " +
                                                                "inferred concepts in rule conclusions.");
            }
        });
    }

}
