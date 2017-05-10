package ai.grakn.graql.internal.reasoner.query;

/**
 * Created by kasper on 10/05/17.
 */

import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.explanation.LookupExplanation;
import ai.grakn.graql.internal.reasoner.explanation.RuleExplanation;
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tuple-at-a-time iterator for this atomic query.
 * Resolves the atomic query by:
 * 1) doing DB lookup
 * 2) applying a rule
 * 3) doing a lemma (previously derived answer) lookup
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

        LOG.debug("AQ: " + query);

        Pair<Stream<Answer>, Unifier> streamUnifierPair = query.lookupWithUnifier(cache);
        this.queryIterator = streamUnifierPair.getKey().iterator();
        this.cacheUnifier = streamUnifierPair.getValue().invert();

        this.ruleIterator = subGoals.contains(query)? Collections.emptyIterator() : query.getRuleIterator();

        //if this already has full substitution and exists in the db then do not resolve further
        /*
        boolean hasFullSubstitution = query.hasFullSubstitution();
        if(subGoals.contains(query)
                || (hasFullSubstitution && queryIterator.hasNext() ) ){
            this.ruleIterator = Collections.emptyIterator();
        }
        else {
            this.ruleIterator = query.getRuleIterator();
        }
        */

        //mark as visited and hence not admissible
        if (ruleIterator.hasNext()) subGoals.add(query);
    }

    @Override
    public boolean hasNext() {
        if (queryIterator.hasNext()) return true;
        else{
            if (ruleIterator.hasNext()) {
                currentRule = ruleIterator.next();
                LOG.debug("Created resolution plan for rule: " + currentRule.getHead().getAtom() + ", id: " + currentRule.getRuleId());
                LOG.debug(currentRule.getBody().getResolutionPlan());
                //TODO: empty sub as the sub is propagated in rule.propagateConstraints method
                queryIterator = currentRule.getBody().iterator(new QueryAnswer(), subGoals, cache);
                return hasNext();
            }
            else return false;
        }
    }

    @Override
    public Answer next() {
        Answer sub = queryIterator.next()
                .merge(query.getSubstitution())
                .filterVars(query.getVarNames());

        //assign appropriate explanation
        if (sub.getExplanation().isLookupExplanation()) sub = sub.explain(new LookupExplanation(query));
        else sub = sub.explain(new RuleExplanation(currentRule));

        LOG.debug("Answer to: " + query);
        LOG.debug(sub.toString());
        return cache.recordAnswerWithUnifier(query, sub, cacheUnifier);
    }

}
