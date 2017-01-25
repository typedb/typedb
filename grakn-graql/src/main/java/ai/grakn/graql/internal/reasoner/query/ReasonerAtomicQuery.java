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
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 * <p>
 * Base reasoner atomic query. An atomic query is a query constrained to having at most one rule-resolvable atom
 * together with its accompanying constraints (predicates and types)
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class ReasonerAtomicQuery extends ReasonerQueryImpl {

    private Atom atom;
    final private QueryAnswers answers = new QueryAnswers();
    final private QueryAnswers newAnswers = new QueryAnswers();
    private static final Logger LOG = LoggerFactory.getLogger(ReasonerAtomicQuery.class);


    public ReasonerAtomicQuery(Conjunction<VarAdmin> pattern, GraknGraph graph){
        super(pattern, graph);
        atom = selectAtoms().iterator().next();
    }

    public ReasonerAtomicQuery(ReasonerAtomicQuery query){
        this(query, new QueryAnswers());
    }

    public ReasonerAtomicQuery(ReasonerAtomicQuery query, QueryAnswers ans){
        super(query);
        answers.addAll(ans);
    }

    public ReasonerAtomicQuery(Atom at) {
        super(at);
        atom = selectAtoms().stream().findFirst().orElse(null);
    }
    
    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + 37;
    }

    /**
     * @return the atom constituting this atomic query
     */
    public Atom getAtom(){ return atom;}

    @Override
    public boolean addAtom(Atomic at) {
        if(super.addAtom(at)){
            if(atom == null && at.isSelectable()) atom = (Atom) at;
            return true;
        }
        else return false;
    }

    @Override
    public boolean removeAtom(Atomic at) {
        if( super.removeAtom(at)) {
            if (atom != null & at.equals(atom)) atom = null;
            return true;
        }
        else return false;
    }

    @Override
    public void unify(Map<VarName, VarName> unifiers) {
        super.unify(unifiers);
        atom = selectAtoms().iterator().next();
    }

    @Override
    public Set<Atom> selectAtoms() {
        Set<Atom> selectedAtoms = super.selectAtoms();
        if (selectedAtoms.size() != 1) {
            throw new IllegalStateException(ErrorMessage.NON_ATOMIC_QUERY.getMessage(this.toString()));
        }
        return selectedAtoms;
    }
    
    public QueryAnswers getAnswers(){ return answers;}
    public QueryAnswers getNewAnswers(){ return newAnswers;}

    /**
     * resolve the query by performing either a db or memory lookup, depending on which is more appropriate
     * @param cache container of already performed query resolutions
     */
    public void lookup(QueryCache cache){
        boolean queryVisited = cache.contains(this);
        if (!queryVisited){
            this.DBlookup();
            cache.record(this);
        }
        else this.memoryLookup(cache);
    }

    /**
     * resolve the query by performing a db lookup
     */
    public void DBlookup() {
        QueryAnswers lookup = new QueryAnswers(getMatchQuery().admin().streamWithVarNames().collect(Collectors.toList()));
        lookup.removeAll(answers);
        answers.addAll(lookup);
        newAnswers.addAll(lookup);
    }

    /**
     * resolve the query by performing a memory (cache) lookup
     * @param cache container of already performed query resolutions
     */
    public void memoryLookup(QueryCache cache) {
        ReasonerAtomicQuery equivalentQuery = cache.get(this);
        if(equivalentQuery != null) {
            QueryAnswers lookup = QueryAnswers.getUnifiedAnswers(this, equivalentQuery);
            lookup.removeAll(answers);
            answers.addAll(lookup);
            newAnswers.addAll(lookup);
        }
    }

    /**
     * execute insert on the query and return inserted answers
     */
    private QueryAnswers insert() {
        QueryAnswers insertAnswers = new QueryAnswers(getMatchQuery().admin().streamWithVarNames().collect(Collectors.toList()));
        if(insertAnswers.isEmpty()){
            InsertQuery insert = Graql.insert(getPattern().getVars()).withGraph(graph());
            insert.stream()
                    .map( m ->
                        m.entrySet().stream()
                        .collect(Collectors.toMap(k -> VarName.of(k.getKey()), Map.Entry::getValue)))
                    .forEach(insertAnswers::add);
       }
       return insertAnswers;
    }
    
    private QueryAnswers materialiseDirect() {
        QueryAnswers insertAnswers = new QueryAnswers();

        //extrapolate if needed
        if(atom.isRelation()){
            Relation relAtom = (Relation) atom;
            Set<VarName> rolePlayers = relAtom.getRolePlayers();
            if (relAtom.getRoleVarTypeMap().size() != rolePlayers.size()) {
                RelationType relType = (RelationType) relAtom.getType();
                Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());
                Set<Map<VarName, Var>> roleMaps = new HashSet<>();
                Utility.computeRoleCombinations(rolePlayers , roles, new HashMap<>(), roleMaps);

                roleMaps.forEach(roleMap -> {
                    Relation relationWithRoles = new Relation(relAtom.getVarName(), relAtom.getValueVariable(),
                            roleMap, relAtom.getPredicate(), this);
                    this.removeAtom(relAtom);
                    this.addAtom(relationWithRoles);
                    insertAnswers.addAll(this.insert());
                    this.removeAtom(relationWithRoles);
                    this.addAtom(relAtom);
                });
            }
            else {
                insertAnswers.addAll(this.insert());
            }
        }
        else {
            insertAnswers.addAll(this.insert());
        }
        return insertAnswers;
    }

    /**
     * @return materialised complete answers (with all ids)
     */
    public QueryAnswers materialise(){
        QueryAnswers fullAnswers = new QueryAnswers();
        ReasonerAtomicQuery queryToMaterialise = new ReasonerAtomicQuery(this);
        answers.forEach(answer -> {
            Set<IdPredicate> subs = new HashSet<>();
            answer.forEach((var, con) -> subs.add(new IdPredicate(var, con, queryToMaterialise)));
            subs.forEach(queryToMaterialise::addAtom);
            fullAnswers.addAll(queryToMaterialise.materialiseDirect());
            subs.forEach(queryToMaterialise::removeAtom);
        });
        return fullAnswers;
    }
    
    private QueryAnswers propagateIdPredicates(QueryAnswers answers){
        QueryAnswers newAnswers = new QueryAnswers();
        if(answers.isEmpty()) return newAnswers;

        Set<IdPredicate> extraSubs = new HashSet<>();
        this.getTypeConstraints().stream()
                .map(TypeAtom::getPredicate).filter(Objects::nonNull)
                .forEach(extraSubs::add);

        answers.forEach( map -> {
            Map<VarName, Concept> newAns = new HashMap<>(map);
            extraSubs.forEach(sub -> newAns.put(sub.getVarName(), graph().getConcept(sub.getPredicate())) );
            newAnswers.add(newAns);
        });
        return newAnswers;
    }
    
    /**
     * attempt query resolution via application of a specific rule
     * @param rl rule through which to resolve the query
     * @param subGoals set of visited subqueries
     * @param cache collection of performed query resolutions
     * @param materialise materialisation flag
     */
    private void resolveViaRule(Rule rl, Set<ReasonerAtomicQuery> subGoals, QueryCache cache, boolean materialise){
        Atom atom = this.getAtom();
        InferenceRule rule = new InferenceRule(rl, graph());
        rule.unify(atom);
        ReasonerQueryImpl ruleBody = rule.getBody();
        ReasonerAtomicQuery ruleHead = rule.getHead();

        Set<Atom> atoms = ruleBody.selectAtoms();
        Iterator<Atom> atIt = atoms.iterator();

        subGoals.add(this);
        ReasonerAtomicQuery childAtomicQuery = new ReasonerAtomicQuery(atIt.next());
        QueryAnswers subs = childAtomicQuery.answer(subGoals, cache, materialise);
        while(atIt.hasNext()){
            childAtomicQuery = new ReasonerAtomicQuery(atIt.next());
            QueryAnswers localSubs = childAtomicQuery.answer(subGoals, cache, materialise);
            subs = subs.join(localSubs);
        }

        QueryAnswers answers = subs
                .filterNonEquals(ruleBody)
                .filterVars(ruleHead.getVarNames())
                .filterKnown(this.getAnswers());

        if (materialise || ruleHead.getAtom().requiresMaterialisation()){
            answers = new ReasonerAtomicQuery(ruleHead, answers).materialise();
        }

        QueryAnswers filteredAnswers = this.propagateIdPredicates(answers)
                .filterVars(this.getVarNames())
                .permute(this.getAtom(), ruleHead.getAtom());
        this.getAnswers().addAll(filteredAnswers);
        this.newAnswers.addAll(filteredAnswers);
        cache.record(this);
    }

    /**
     * answer the query by providing combined lookup/rule resolutions
     * @param subGoals set of visited subqueries
     * @param cache collection of performed query resolutions
     * @param materialise materialisation flag
     * @return answers to the query
     */
    public QueryAnswers answer(Set<ReasonerAtomicQuery> subGoals, QueryCache cache, boolean materialise){
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
    public Stream<Map<VarName, Concept>> resolve(boolean materialise) {
        if (!this.getAtom().isRuleResolvable()) {
            return this.getMatchQuery().admin().streamWithVarNames();
        } else {
            return new ReasonerAtomicQuery.QueryAnswerIterator(materialise).hasStream();
        }
    }

    private class QueryAnswerIterator implements Iterator<Map<VarName, Concept>> {

        private int dAns = 0;
        private int iter = 0;
        private final boolean materialise;
        private final QueryCache cache = new QueryCache();
        private final Set<ReasonerAtomicQuery> subGoals = new HashSet<>();
        private final Set<Rule> rules;
        private Iterator<Map<VarName, Concept>> answerIterator = Collections.emptyIterator();
        private Iterator<Rule> ruleIterator = Collections.emptyIterator();

        public QueryAnswerIterator(boolean materialise){
            this.materialise = materialise;
            this.rules = outer().getAtom().getApplicableRules();
            LOG.debug("Atom: " + outer().getAtom() + " applicable rules: " + rules.size());
            lookup(cache);
            this.answerIterator = outer().newAnswers.iterator();
        }

        /**
         * @return stream constructed out of the answer iterator
         */
        public Stream<Map<VarName, Concept>> hasStream(){
            Iterable<Map<VarName, Concept>> iterable = () -> this;
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
        }

        private void computeNext(){
            if (!hasNextRule()) initIteration();
            outer().newAnswers.clear();
            Rule rule = nextRule();
            LOG.debug("Resolving rule: " + rule.getId() + " answers: " + size());
            outer().resolveViaRule(rule, subGoals, cache, materialise);
            if (!hasNextRule()) completeIteration();
            answerIterator = outer().getNewAnswers().iterator();
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
            } else {
                return false;
            }
        }

        /**
         * @return single answer to the query
         */
        public Map<VarName, Concept> next() { return answerIterator.next();}
        private ReasonerAtomicQuery outer(){ return ReasonerAtomicQuery.this;}
        private int size(){ return outer().getAnswers().size();}
    }
}
