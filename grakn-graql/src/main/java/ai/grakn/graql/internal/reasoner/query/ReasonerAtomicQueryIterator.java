/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.explanation.RuleExplanation;
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleTuple;

import java.util.stream.StreamSupport;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 * <p>
 * Tuple-at-a-time iterator for atomic queries.
 * Resolves the atomic query by:
 * 1) doing DB lookup
 * 2) applying a rule
 * 3) doing a lemma (previously derived answer) lookup from query cache
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
class ReasonerAtomicQueryIterator extends ReasonerQueryIterator {

    private final ReasonerAtomicQuery query;

    private final QueryCache<ReasonerAtomicQuery> cache;
    private final Set<ReasonerAtomicQuery> subGoals;
    private final Iterator<RuleTuple> ruleIterator;
    private Iterator<Answer> queryIterator = Collections.emptyIterator();

    private Unifier cacheUnifier = new UnifierImpl();

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerAtomicQuery.class);

    ReasonerAtomicQueryIterator(ReasonerAtomicQuery q, Answer sub, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> qc){
        this.subGoals = subGoals;
        this.cache = qc;
        this.query = new ReasonerAtomicQuery(q);

        query.addSubstitution(sub);

        LOG.debug("AQ: " + query);

        Pair<Stream<Answer>, Unifier> streamUnifierPair = query.lookupWithUnifier(cache);
        this.queryIterator = streamUnifierPair.getKey()
                .map(a -> a.explain(a.getExplanation().setQuery(query)))
                .iterator();
        this.cacheUnifier = streamUnifierPair.getValue().inverse();

        //if this already has full substitution and exists in the db then do not resolve further
        //NB: the queryIterator check is purely because we may want to ask for an explanation
        boolean hasFullSubstitution = query.hasFullSubstitution();
        if(subGoals.contains(query)
                || (hasFullSubstitution && queryIterator.hasNext() ) ){
            this.ruleIterator = Collections.emptyIterator();
        }
        else {
            this.ruleIterator = query.getRuleIterator();
        }

        //mark as visited and hence not admissible
        if (ruleIterator.hasNext()) subGoals.add(query);
    }

    private Iterator<Answer> getRuleQueryIterator(RuleTuple rc){

        InferenceRule rule = rc.getRule();
        Unifier ruleUnifier = rc.getRuleUnifier();
        Unifier permutationUnifier = rc.getPermutationUnifier();

        //delta' = theta . thetaP . delta
        Answer sub = query.getSubstitution();
        Unifier uInv = ruleUnifier.inverse();
        Answer partialSubPrime = sub
                .unify(permutationUnifier)
                .unify(uInv);

        Set<Var> varsToRetain = rule.hasDisconnectedHead()?
                rule.getBody().getVarNames() :
                rule.getHead().getVarNames();

        Unifier combinedUnifier = ruleUnifier.combine(permutationUnifier);

        Iterable<Answer> baseIterable = () -> rule.getBody().iterator(partialSubPrime, subGoals, cache);
        Stream<Answer> baseStream = StreamSupport
                .stream(baseIterable.spliterator(), false)
                .map(a -> a.filterVars(varsToRetain));

        Stream<Answer> ruleStream = rule.requiresMaterialisation(query.getAtom())?
                getMaterialisedRuleStream(baseStream, sub, rule, combinedUnifier) :
                getRuleStream(baseStream, sub, rule, combinedUnifier);

        return ruleStream.iterator();
    }

    private Stream<Answer> getRuleStream(Stream<Answer> baseStream, Answer sub, InferenceRule rule, Unifier unifier){
        Set<Var> queryVars = query.getVarNames();
        return baseStream
                .map(a -> a.unify(unifier))
                .filter(a -> !a.isEmpty())
                .map(a -> a.merge(sub))
                .map(a -> a.filterVars(queryVars))
                .map(a -> a.explain(new RuleExplanation(query, rule)));
    }

    private Stream<Answer> getMaterialisedRuleStream(Stream<Answer> baseStream, Answer sub, InferenceRule rule, Unifier unifier){
        ReasonerAtomicQuery ruleHead = rule.getHead();
        Set<Var> queryVars = query.getVarNames().size() < ruleHead.getVarNames().size()? unifier.keySet() : ruleHead.getVarNames();
        baseStream = baseStream
                .distinct()
                .map(a -> {
                    boolean queryEquivalentToHead = query.isEquivalent(ruleHead);

                    //check if the specific answer to ruleHead already in cache/db
                    Answer headAnswer = ruleHead
                            .lookupAnswer(cache, a)
                            .filterVars(queryVars)
                            .unify(unifier);

                    //if not and query different than rule head do the same with the query
                    Answer queryAnswer = headAnswer.isEmpty() && queryEquivalentToHead?
                            query.lookupAnswer(cache, a) :
                            new QueryAnswer();

                    //ensure no duplicates created - only materialise answer if it doesn't exist in the db
                    if (headAnswer.isEmpty()
                        && queryAnswer.isEmpty()) {
                        Answer materialisedSub = ruleHead.materialise(a).findFirst().orElse(null);
                        if (!queryEquivalentToHead) cache.recordAnswer(ruleHead, materialisedSub);
                        return materialisedSub
                                .filterVars(queryVars)
                                .unify(unifier);
                    } else {
                        return headAnswer.isEmpty()? queryAnswer : headAnswer;
                    }
                });
        return baseStream
                .filter(a -> !a.isEmpty())
                .map(a -> a.merge(sub))
                .map(a -> a.explain(new RuleExplanation(query, rule)));
    }

    @Override
    public boolean hasNext() {
        if (queryIterator.hasNext()) return true;
        else{
            if (ruleIterator.hasNext()) {
                queryIterator = getRuleQueryIterator(ruleIterator.next());
                return hasNext();
            }
            else return false;
        }
    }

    @Override
    public Answer next() {
        Answer sub = queryIterator.next();
        sub  = cache.recordAnswerWithUnifier(query, sub, cacheUnifier);
        return sub;
    }

}
