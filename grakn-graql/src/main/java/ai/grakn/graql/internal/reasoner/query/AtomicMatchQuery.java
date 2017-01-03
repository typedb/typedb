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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Rule;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * <p>
 * Atomic reasoner query providing resolution streaming facilities.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class AtomicMatchQuery extends AtomicQuery{

    final private QueryAnswers answers;
    final private QueryAnswers newAnswers;
    private static final Logger LOG = LoggerFactory.getLogger(AtomicQuery.class);

    public AtomicMatchQuery(Atom atom, Set<String> vars){
        super(atom, vars);
        answers = new QueryAnswers();
        newAnswers = new QueryAnswers();
    }

    public AtomicMatchQuery(MatchQuery query, GraknGraph graph){
        super(query, graph);
        answers = new QueryAnswers();
        newAnswers = new QueryAnswers();
    }

    public AtomicMatchQuery(AtomicQuery query, QueryAnswers ans){
        super(query);
        answers = new QueryAnswers(ans);
        newAnswers = new QueryAnswers();
    }

    @Override
    public Stream<Map<String, Concept>> stream() {return answers.stream();}

    @Override
    public QueryAnswers getAnswers(){ return answers;}
    @Override
    public QueryAnswers getNewAnswers(){ return newAnswers;}

    @Override
    public void lookup(QueryCache cache){
        boolean queryVisited = cache.contains(this);
        if (!queryVisited){
            this.DBlookup();
            cache.record(this);
        }
        else this.memoryLookup(cache);
    }

    @Override
    public void DBlookup() {
        QueryAnswers lookup = new QueryAnswers(execute());
        lookup.removeAll(answers);
        answers.addAll(lookup);
        newAnswers.addAll(lookup);
    }

    @Override
    public void memoryLookup(QueryCache cache) {
        AtomicQuery equivalentQuery = cache.get(this);
        if(equivalentQuery != null) {
            QueryAnswers lookup = QueryAnswers.getUnifiedAnswers(this, equivalentQuery, equivalentQuery.getAnswers());
            lookup.removeAll(answers);
            answers.addAll(lookup);
            newAnswers.addAll(lookup);
        }
    }

    @Override
    public void propagateAnswers(QueryCache cache) {
        getChildren().forEach(childQuery -> {
            QueryAnswers ans = QueryAnswers.getUnifiedAnswers(childQuery, this, cache.get(this).getAnswers());
            childQuery.getAnswers().addAll(ans);
            childQuery.getNewAnswers().addAll(ans);
            childQuery.propagateAnswers(cache);
        });
    }

    @Override
    public QueryAnswers materialise(){
        QueryAnswers fullAnswers = new QueryAnswers();
        answers.forEach(answer -> {
            Set<IdPredicate> subs = new HashSet<>();
            answer.forEach((var, con) -> {
                IdPredicate sub = new IdPredicate(var, con);
                if (!containsAtom(sub))
                    subs.add(sub);
            });
            fullAnswers.addAll(materialise(subs));
        });
        return fullAnswers;
    }

    private QueryAnswers propagateHeadIdPredicates(Query ruleHead, QueryAnswers answers){
        QueryAnswers newAnswers = new QueryAnswers();
        if(answers.isEmpty()) return newAnswers;

        Set<String> queryVars = getSelectedNames();
        Set<String> headVars = ruleHead.getSelectedNames();
        Set<Predicate> extraSubs = new HashSet<>();
        if(queryVars.size() > headVars.size()){
            extraSubs.addAll(ruleHead.getIdPredicates()
                    .stream().filter(sub -> queryVars.contains(sub.getVarName()))
                    .collect(Collectors.toSet()));
        }

        answers.forEach( map -> {
            Map<String, Concept> newAns = new HashMap<>(map);
            extraSubs.forEach(sub -> newAns.put(sub.getVarName(), graph().getConcept(sub.getPredicateValue())) );
            newAnswers.add(newAns);
        });
        return newAnswers;
    }

    @Override
    public void resolveViaRule(Rule rl, Set<AtomicQuery> subGoals, QueryCache cache, boolean materialise){
        Atom atom = this.getAtom();
        InferenceRule rule = new InferenceRule(rl, graph());
        rule.unify(atom);
        Query ruleBody = rule.getBody();
        AtomicQuery ruleHead = rule.getHead();

        Set<Atom> atoms = ruleBody.selectAtoms();
        Iterator<Atom> atIt = atoms.iterator();

        subGoals.add(this);
        AtomicQuery childAtomicQuery = new AtomicMatchQuery(atIt.next(), this.getSelectedNames());
        if(!materialise) this.establishRelation(childAtomicQuery);
        QueryAnswers subs = childAtomicQuery.answer(subGoals, cache, materialise);
        while(atIt.hasNext()){
            childAtomicQuery = new AtomicMatchQuery(atIt.next(), getSelectedNames());
            if(!materialise) this.establishRelation(childAtomicQuery);
            QueryAnswers localSubs = childAtomicQuery.answer(subGoals, cache, materialise);
            subs = subs.join(localSubs);
        }

        QueryAnswers answers = this.propagateHeadIdPredicates(ruleHead, subs)
                .filterNonEquals(ruleBody)
                .filterVars(ruleHead.getSelectedNames())
                .filterKnown(this.getAnswers());

        QueryAnswers newAnswers = new QueryAnswers();
        if (materialise || ruleHead.getAtom().requiresMaterialisation())
            newAnswers.addAll(new AtomicMatchQuery(ruleHead, answers).materialise());
        if (!newAnswers.isEmpty()){
            if (materialise) answers = newAnswers;
            else answers = answers.join(newAnswers);
        }

        //TODO do all combinations if roles missing
        QueryAnswers filteredAnswers = answers
                .filterVars(this.getSelectedNames())
                .filterIncomplete(this.getSelectedNames());
        this.getAnswers().addAll(filteredAnswers);
        this.newAnswers.addAll(filteredAnswers);
        cache.record(this);
    }

    @Override
    public QueryAnswers answer(Set<AtomicQuery> subGoals, QueryCache cache, boolean materialise){
        boolean queryAdmissible = !subGoals.contains(this);
        lookup(cache);
        if(queryAdmissible) {
            Atom atom = this.getAtom();
            Set<Rule> rules = atom.getApplicableRules();
            rules.forEach(rule -> resolveViaRule(rule, subGoals, cache, materialise));
        }
        return this.getAnswers();
    }

    @Override
    public Stream<Map<String, Concept>> resolve(boolean materialise) {
        if (!this.getAtom().isRuleResolvable())
            return this.getMatchQuery().stream();
        else
            return new QueryAnswerIterator(materialise).hasStream();
    }

    private class QueryAnswerIterator implements Iterator<Map<String, Concept>> {

        private int dAns = 0;
        private int iter = 0;
        private final boolean materialise;
        private final QueryCache cache = new QueryCache();
        private final Set<AtomicQuery> subGoals = new HashSet<>();
        private final Set<Rule> rules;
        private Iterator<Map<String, Concept>> answerIterator = Collections.emptyIterator();
        private Iterator<Rule> ruleIterator = Collections.emptyIterator();

        public QueryAnswerIterator(boolean materialise){
            this.materialise = materialise;
            this.rules = outer().getAtom().getApplicableRules();
            lookup(cache);
            this.answerIterator = outer().newAnswers.iterator();
        }

        /**
         * @return stream constructed out of the answer iterator
         */
        public Stream<Map<String, Concept>> hasStream(){
            Iterable<Map<String, Concept>> iterable = () -> this;
            return StreamSupport.stream(iterable.spliterator(), false);
        }

        private boolean hasNextRule(){ return ruleIterator.hasNext();}
        private Rule nextRule(){return ruleIterator.next();}

        private void initIteration(){
            ruleIterator = rules.iterator();
            dAns = size();
            subGoals.clear();
        }

        private void completeIteration(){
            LOG.debug("Atom: " + outer().getAtom() + " iter: " + iter + " answers: " + size());
            dAns = size() - dAns;
            iter++;
            if (!materialise) cache.propagateAnswers();
        }

        private void computeNext(){
            if (!hasNextRule()) initIteration();
            outer().newAnswers.clear();
            Rule rule = nextRule();
            LOG.debug("Resolving rule: " + rule.getId() + " answers: " + size());
            outer().resolveViaRule(rule, subGoals, cache, materialise);
            if (!hasNextRule()) completeIteration();

            answerIterator = outer().newAnswers.iterator();
        }

        /**
         * check whether answers available, if answers not fully computed compute more answers
         * @return true if answers available
         */
        public boolean hasNext() {
            if (answerIterator.hasNext()) return true;
            else if (dAns != 0 || iter == 0 ){
                computeNext();
                return hasNext();
            }
            else
                return false;
        }

        /**
         * @return single answer to the query
         */
        public Map<String, Concept> next() { return answerIterator.next();}
        private AtomicMatchQuery outer(){ return AtomicMatchQuery.this;}
        private int size(){ return outer().getAnswers().size();}
    }
}
