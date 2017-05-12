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

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.explanation.LookupExplanation;
import ai.grakn.graql.internal.reasoner.explanation.RuleExplanation;
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Iterator<InferenceRule> ruleIterator;
    private Iterator<Answer> queryIterator = Collections.emptyIterator();
    private Unifier cacheUnifier = new UnifierImpl();

    private InferenceRule currentRule = null;

    private static final Logger LOG = LoggerFactory.getLogger(ReasonerAtomicQuery.class);

    ReasonerAtomicQueryIterator(ReasonerAtomicQuery q, Answer sub, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> qc){
        this.subGoals = subGoals;
        this.cache = qc;
        this.query = new ReasonerAtomicQuery(q);

        query.addSubstitution(sub);

        LOG.trace("AQ: " + query);
        LOG.trace("AQ delta: " + sub);

        Pair<Stream<Answer>, Unifier> streamUnifierPair = query.lookupWithUnifier(cache);
        this.queryIterator = streamUnifierPair.getKey().iterator();
        this.cacheUnifier = streamUnifierPair.getValue().invert();

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

    @Override
    public boolean hasNext() {
        if (queryIterator.hasNext()) return true;

        if (ruleIterator.hasNext()) {
            currentRule = ruleIterator.next();
            LOG.trace("Applying rule: " + currentRule.getHead().getAtom() + ", id: " + currentRule.getRuleId());
            //TODO: empty sub as the sub is propagated in rule.propagateConstraints method
            queryIterator = currentRule.getBody().iterator(new QueryAnswer(), subGoals, cache);
            return hasNext();
        }
        else return false;
    }

    @Override
    public Answer next() {
        Answer sub = queryIterator.next()
                .merge(query.getSubstitution())
                .filterVars(query.getVarNames());

        //assign appropriate explanation
        if (sub.getExplanation().isLookupExplanation()) sub = sub.explain(new LookupExplanation(query));
        else sub = sub.explain(new RuleExplanation(currentRule));

        return cache.recordAnswerWithUnifier(query, sub, cacheUnifier);
    }

}
