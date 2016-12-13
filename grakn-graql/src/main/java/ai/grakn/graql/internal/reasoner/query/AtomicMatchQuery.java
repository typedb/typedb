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

import ai.grakn.concept.Concept;
import ai.grakn.concept.Rule;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import com.google.common.collect.Iterators;
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

public class AtomicMatchQuery extends AtomicQuery{

    //final private Stream<Map<String, Concept>> answers;
    final private QueryAnswers answers;
    final private QueryAnswers newAnswers;
    private static final Logger LOG = LoggerFactory.getLogger(AtomicQuery.class);

    public AtomicMatchQuery(Atom atom, Set<String> vars){
        super(atom, vars);
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
    public void lookup(){

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

    public QueryAnswers propagateHeadIdPredicates(Query ruleHead, QueryAnswers answers){
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
    protected QueryAnswers answerWM(Set<AtomicQuery> subGoals, QueryCache cache){
        boolean queryAdmissible = !subGoals.contains(this);
        boolean queryVisited = cache.contains(this);

        if(queryAdmissible) {
            if (!queryVisited){
                this.DBlookup();
                cache.record(this);
            }
            else
                this.memoryLookup(cache);

            Atom atom = this.getAtom();
            Set<Rule> rules = atom.getApplicableRules();
            for (Rule rl : rules) {
                InferenceRule rule = new InferenceRule(rl, graph());
                rule.unify(atom);
                Query ruleBody = rule.getBody();
                AtomicQuery ruleHead = rule.getHead();

                Set<Atom> atoms = ruleBody.selectAtoms();
                Iterator<Atom> atIt = atoms.iterator();

                subGoals.add(this);
                AtomicQuery childAtomicQuery = new AtomicMatchQuery(atIt.next(), this.getSelectedNames());
                QueryAnswers subs = childAtomicQuery.answerWM(subGoals, cache);
                while(atIt.hasNext()){
                    childAtomicQuery = new AtomicMatchQuery(atIt.next(), getSelectedNames());
                    QueryAnswers localSubs = childAtomicQuery.answerWM(subGoals, cache);
                    subs = subs.join(localSubs);
                }

                QueryAnswers answers = this.propagateHeadIdPredicates(ruleHead, subs)
                        .filterNonEquals(ruleBody)
                        .filterVars(ruleHead.getSelectedNames())
                        .filterKnown(this.getAnswers());
                QueryAnswers newAnswers = new QueryAnswers();
                newAnswers.addAll(new AtomicMatchQuery(ruleHead, answers).materialise());

                if (!newAnswers.isEmpty()) answers = newAnswers;
                //TODO do all combinations if roles missing
                QueryAnswers filteredAnswers = answers
                        .filterVars(this.getSelectedNames())
                        .filterIncomplete(this.getSelectedNames());
                this.getAnswers().addAll(filteredAnswers);
                cache.record(this);
            }
        }
        else
            this.memoryLookup(cache);

        return this.getAnswers();
    }

    @Override
    protected QueryAnswers answer(Set<AtomicQuery> subGoals, QueryCache cache){
        boolean queryAdmissible = !subGoals.contains(this);
        boolean queryVisited = cache.contains(this);


        if(queryAdmissible) {
            if (!queryVisited){
                this.DBlookup();
                cache.record(this);
            }
            else
                this.memoryLookup(cache);

            Atom atom = this.getAtom();
            Set<Rule> rules = atom.getApplicableRules();
            for (Rule rl : rules) {
                InferenceRule rule = new InferenceRule(rl, graph());
                rule.unify(atom);
                Query ruleBody = rule.getBody();
                AtomicQuery ruleHead = rule.getHead();

                Set<Atom> atoms = ruleBody.selectAtoms();
                Iterator<Atom> atIt = atoms.iterator();

                subGoals.add(this);
                Atom at = atIt.next();
                AtomicQuery childAtomicQuery = new AtomicMatchQuery(at, this.getSelectedNames());
                this.establishRelation(childAtomicQuery);
                QueryAnswers subs = childAtomicQuery.answer(subGoals, cache);
                while (atIt.hasNext()) {
                    at = atIt.next();
                    childAtomicQuery = new AtomicMatchQuery(at, this.getSelectedNames());
                    this.establishRelation(childAtomicQuery);
                    QueryAnswers localSubs = childAtomicQuery.answer(subGoals, cache);
                    subs = subs.join(localSubs);
                }

                QueryAnswers answers = this.propagateHeadIdPredicates(ruleHead, subs)
                        .filterNonEquals(ruleBody)
                        .filterVars(ruleHead.getSelectedNames())
                        .filterKnown(this.getAnswers());
                QueryAnswers newAnswers = new QueryAnswers();
                if (atom.isResource()
                        || atom.isUserDefinedName() && atom.getType().isRelationType() )
                    newAnswers.addAll(new AtomicMatchQuery(ruleHead, answers).materialise());
                if (!newAnswers.isEmpty()) answers = answers.join(newAnswers);

                QueryAnswers filteredAnswers = answers
                        .filterVars(this.getSelectedNames())
                        .filterIncomplete(this.getSelectedNames());
                this.getAnswers().addAll(filteredAnswers);
                this.newAnswers.addAll(filteredAnswers);
                cache.record(this);
            }
        }
        else
            this.memoryLookup(cache);

        return this.getAnswers();
    }

    @Override
    public void answer(Set<AtomicQuery> subGoals, QueryCache cache, boolean materialise){
        if(!materialise) {
            this.answer(subGoals, cache);
            cache.propagateAnswers();
        }
        else
            this.answerWM(subGoals, cache);
    }

    @Override
    public Stream<Map<String, Concept>> resolve(boolean materialise) {
        if (!this.getAtom().isRuleResolvable()){
            this.DBlookup();
            return this.stream();
        }
        else {
            QueryAnswerIterator it = new QueryAnswerIterator(materialise);
            return it.hasStream();
        }
    }

    private class QueryAnswerIterator implements Iterator<Map<String, Concept>> {

        boolean materialise;
        private int dAns = 0;
        private int iter = 0;
        private QueryCache cache = new QueryCache();
        private Set<AtomicQuery> subGoals = new HashSet<>();
        Iterator<Map<String, Concept>> answers = Collections.emptyIterator();

        public QueryAnswerIterator(boolean materialise){
            this.materialise = materialise;
        }

        public Stream<Map<String, Concept>> hasStream(){
            Iterable<Map<String, Concept>> iterable = () -> this;
            return StreamSupport.stream(iterable.spliterator(), false);
        }

        private void computeNext(){
            dAns = size();
            outer().newAnswers.clear();
            subGoals.clear();
            outer().answer(subGoals, cache, materialise);
            LOG.debug("Atom: " + outer().getAtom() + " iter: " + iter++ + " answers: " + outer().getAnswers().size());
            dAns = size() - dAns;
            answers = outer().newAnswers.iterator();
        }

        public boolean hasNext() {
            if (answers.hasNext()) return true;
            else if ( dAns != 0 || iter == 0){
                computeNext();
                return hasNext();
            }
            else
                return false;
        }

        public Map<String, Concept> next() { return answers.next();}
        private AtomicMatchQuery outer(){ return AtomicMatchQuery.this;}
        private int size(){ return outer().getAnswers().size();}
    }
}
