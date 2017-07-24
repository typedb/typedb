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
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.cache.Cache;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.explanation.LookupExplanation;
import ai.grakn.graql.internal.reasoner.explanation.RuleExplanation;
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.graql.internal.reasoner.rule.RuleTuple;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.entityTypeFilter;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.knownFilterWithInverse;

/**
 *
 * <p>
 * Base reasoner atomic query. An atomic query is a query constrained to having at most one rule-resolvable atom
 * together with its accompanying constraints (predicates and types).
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class ReasonerAtomicQuery extends ReasonerQueryImpl {

    private Atom atom;
    private static final Logger LOG = LoggerFactory.getLogger(ReasonerAtomicQuery.class);

    ReasonerAtomicQuery(Conjunction<VarPatternAdmin> pattern, GraknGraph graph) {
        super(pattern, graph);
        atom = selectAtoms().stream().findFirst().orElse(null);
    }

    ReasonerAtomicQuery(ReasonerQueryImpl query) {
        super(query);
        atom = selectAtoms().stream().findFirst().orElse(null);
    }

    ReasonerAtomicQuery(Atom at) {
        super(at);
        atom = selectAtoms().stream().findFirst().orElse(null);
    }

    @Override
    public ReasonerQuery copy(){ return new ReasonerAtomicQuery(this);}

    @Override
    public boolean equals(Object obj) {
        return !(obj == null || this.getClass() != obj.getClass()) && super.equals(obj);
    }

    @Override
    public String toString(){
        return getAtoms().stream()
                .filter(Atomic::isAtom)
                .map(Atomic::toString).collect(Collectors.joining(", "));
    }

    @Override
    public int hashCode() {
        return super.hashCode() + 37;
    }

    @Override
    public boolean isAtomic(){ return true;}

    /**
     * @return the atom constituting this atomic query
     */
    public Atom getAtom() {
        return atom;
    }


    @Override
    public boolean addAtomic(Atomic at) {
        if (super.addAtomic(at)) {
            if (atom == null && at.isSelectable()) atom = (Atom) at;
            return true;
        } else return false;
    }

    @Override
    public boolean removeAtomic(Atomic at) {
        if (super.removeAtomic(at)) {
            if (at.equals(atom)) atom = null;
            return true;
        } else return false;
    }

    @Override
    public Set<Atom> selectAtoms() {
        Set<Atom> selectedAtoms = super.selectAtoms();
        if (selectedAtoms.size() != 1) {
            throw GraqlQueryException.nonAtomicQuery(this);
        }
        return selectedAtoms;
    }

    @Override
    public Unifier getUnifier(ReasonerQuery p){
        if (p == this) return new UnifierImpl();
        ReasonerAtomicQuery parent = (ReasonerAtomicQuery) p;
        Unifier unifier = getAtom().getUnifier(parent.getAtom());
        //get type unifiers
        Set<Atom> unified = new HashSet<>();
        getAtom().getTypeConstraints()
                .forEach(type -> {
                    Set<Atom> toUnify = Sets.difference(parent.getEquivalentAtoms(type), unified);
                    Atom equiv = toUnify.stream().findFirst().orElse(null);
                    //only apply if unambiguous
                    if (equiv != null && toUnify.size() == 1){
                        unifier.merge(type.getUnifier(equiv));
                        unified.add(equiv);
                    }
                });
        return unifier;
    }

    /**
     * resolve the query by performing either a db or memory lookup, depending on which is more appropriate
     * @param cache container of already performed query resolutions
     */
    public Stream<Answer> lookup(Cache<ReasonerAtomicQuery, ?> cache) {
        boolean queryVisited = cache.contains(this);
        return queryVisited ? cache.getAnswerStream(this) : DBlookup(cache);
    }

    /**
     * check whether specific answer to this query exists in cache/db
     * @param cache qieru cache
     * @param sub specific answer
     * @return found answer if any, otherwise empty answer
     */
    Answer lookupAnswer(QueryCache<ReasonerAtomicQuery> cache, Answer sub) {
        boolean queryVisited = cache.contains(this);
        if (queryVisited){
            Answer answer = cache.getAnswer(this, sub);
            if (!answer.isEmpty()) return answer;
        }

        List<Answer> match = new ReasonerAtomicQuery(this).addSubstitution(sub).getMatchQuery().execute();
        return match.isEmpty()? new QueryAnswer() : match.iterator().next();
    }

    Pair<Stream<Answer>, Unifier> lookupWithUnifier(Cache<ReasonerAtomicQuery, ?> cache) {
        boolean queryVisited = cache.contains(this);
        return queryVisited ? cache.getAnswerStreamWithUnifier(this) : new Pair<>(DBlookup(), new UnifierImpl());
    }

    private Stream<Answer> DBlookup() {
        return getMatchQuery().admin().stream()
                .map(QueryAnswer::new)
                .map(a -> a.explain(new LookupExplanation(this)));
    }

    /**
     * resolve the query by performing a db lookup with subsequent cache update
     */
    private Stream<Answer> DBlookup(Cache<ReasonerAtomicQuery, ?> cache) {
        return cache.record(this, DBlookup());
    }

    /**
     * execute insert on the query and return inserted answers
     */
    private Stream<Answer> insert() {
        return Graql.insert(getPattern().getVars()).withGraph(graph()).stream();
    }

    public Stream<Answer> materialise(Answer answer) {
        ReasonerAtomicQuery queryToMaterialise = new ReasonerAtomicQuery(this);
        queryToMaterialise.addSubstitution(answer);
        return queryToMaterialise.insert()
                .map(ans -> ans.setExplanation(answer.getExplanation()));
    }

    private Stream<Answer> getIdPredicateAnswerStream(Stream<Answer> stream){
        Answer idPredicateAnswer = getSubstitution();
        return stream.map(answer -> {
            AnswerExplanation exp = answer.getExplanation();
            return answer.merge(idPredicateAnswer).explain(exp);
        });
    }

    private Stream<Answer> getFilteredAnswerStream(Stream<Answer> answers){
        Set<Var> vars = getVarNames();
        Set<TypeAtom> mappedTypeConstraints = atom.getSpecificTypeConstraints();
        return getIdPredicateAnswerStream(answers)
                .filter(a -> entityTypeFilter(a, mappedTypeConstraints))
                .map(a -> a. filterVars(vars));
    }

    /**
     * attempt query resolution via application of a specific rule
     * @param rule rule to apply to resolve the query
     * @param subGoals set of visited subqueries
     * @param cache collection of performed query resolutions
     * @return answers from rule resolution
     */
    private Stream<Answer> resolveViaRule(InferenceRule rule,
                                          Unifier ruleUnifier,
                                          Unifier permutationUnifier,
                                          Set<ReasonerAtomicQuery> subGoals,
                                          Cache<ReasonerAtomicQuery, ?> cache,
                                          Cache<ReasonerAtomicQuery, ?> dCache,
                                          boolean differentialJoin){

        LOG.trace("Applying rule " + rule.getRuleId());

        ReasonerQueryImpl ruleBody = rule.getBody();
        ReasonerAtomicQuery ruleHead = rule.getHead();
        Set<Var> varsToRetain = rule.hasDisconnectedHead()? ruleBody.getVarNames() : ruleHead.getVarNames();

        subGoals.add(this);
        Stream<Answer> answers = ruleBody
                .computeJoin(subGoals, cache, dCache, differentialJoin)
                .map(a -> a.filterVars(varsToRetain))
                .distinct()
                .map(ans -> ans.explain(new RuleExplanation(this, rule)));

        //materialise
        if (!cache.contains(ruleHead)) dCache.record(ruleHead, ruleHead.lookup(cache));
        //filter known to make sure no duplicates are inserted (put behaviour)
        Map<Pair<Var, Concept>, Set<Answer>> known = cache.getInverseAnswerMap(ruleHead);
        Map<Pair<Var, Concept>, Set<Answer>> dknown = dCache.getInverseAnswerMap(ruleHead);

        answers = answers
                .filter(a -> knownFilterWithInverse(a, known))
                .filter(a -> knownFilterWithInverse(a, dknown))
                .flatMap(ruleHead::materialise);

        answers = dCache.record(ruleHead, answers);

        //unify answers
        boolean isHeadEquivalent = this.isEquivalent(ruleHead);
        Set<Var> queryVars = this.getVarNames().size() < ruleHead.getVarNames().size()? ruleUnifier.keySet() : ruleHead.getVarNames();
        answers = answers
                .map(a -> a.filterVars(queryVars))
                .map(a -> a.unify(ruleUnifier))
                .map(a -> a.unify(permutationUnifier))
                .filter(a -> !a.isEmpty());

        //if query not exactly equal to the rule head, do some conversion
        return  isHeadEquivalent? dCache.record(this, answers) : dCache.record(this, getFilteredAnswerStream(answers));
    }

    /**
     * resolves the query by performing lookups and rule resolution and returns a stream of new answers
     * @param subGoals visited subGoals (recursive queries)
     * @param cache global query cache
     * @param dCache differential query cache
     * @return stream of differential answers
     */
    Stream<Answer> answerStream(Set<ReasonerAtomicQuery> subGoals,
                                       Cache<ReasonerAtomicQuery, ?> cache,
                                       Cache<ReasonerAtomicQuery, ?> dCache,
                                       boolean differentialJoin){
        boolean queryAdmissible = !subGoals.contains(this);

        LOG.trace("AQ: " + this);

        Stream<Answer> answerStream = cache.contains(this) ? Stream.empty() : dCache.record(this, lookup(cache));
        if(queryAdmissible) {

            Iterator<RuleTuple> ruleIterator = getRuleIterator();
            while(ruleIterator.hasNext()) {
                RuleTuple ruleContext = ruleIterator.next();
                InferenceRule rule = ruleContext.getRule();
                Unifier u = ruleContext.getRuleUnifier();
                Unifier pu = ruleContext.getPermutationUnifier();

                Answer sub = this.getSubstitution().unify(u.inverse());
                rule.getHead().addSubstitution(sub);
                rule.getBody().addSubstitution(sub);

                Stream<Answer> localStream = resolveViaRule(rule, u, pu, subGoals, cache, dCache, differentialJoin);
                answerStream = Stream.concat(answerStream, localStream);
            }
        }

        return dCache.record(this, answerStream);
    }

    @Override
    public Stream<Answer> resolveAndMaterialise(LazyQueryCache<ReasonerAtomicQuery> cache, LazyQueryCache<ReasonerAtomicQuery> dCache) {
        if (!this.getAtom().isRuleResolvable()) {
            return this.getMatchQuery().admin().stream().map(QueryAnswer::new);
        } else {
            return new QueryAnswerIterator(cache, dCache).hasStream();
        }
    }

    @Override
    public Iterator<Answer> iterator(Answer sub, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache){
        return extendedIterator(sub, subGoals, cache);
    }

    @Override
    public Iterator<Answer> extendedIterator(Answer sub, Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache){
        Iterator<ReasonerAtomicQueryIterator> qIterator = getQueryStream(sub)
                .map(q -> new ReasonerAtomicQueryIterator(q, sub, subGoals, cache))
                .iterator();
        return Iterators.concat(qIterator);
    }

    /**
     * @return stream of atomic query obtained by inserting all inferred possible types (if ambiguous)
     */
    private Stream<ReasonerAtomicQuery> getQueryStream(Answer sub){
        Atom atom = getAtom();
        if (atom.isRelation() && atom.getOntologyConcept() == null){
            List<RelationType> relationTypes = ((RelationAtom) atom).inferPossibleRelationTypes(sub);
            LOG.trace("AQ: " + this + ": inferred rel types for: " + relationTypes.stream().map(Type::getLabel).collect(Collectors.toList()));
            return relationTypes.stream()
                    .map(((RelationAtom) atom)::addType)
                    .sorted(Comparator.comparing(Atom::isRuleResolvable))
                    .map(ReasonerAtomicQuery::new);
        }
        else{
            return Stream.of(this);
        }
    }

    /**
     * @return iterator of all rules applicable to this atomic query including permuted cases when the role types are meta roles
     */
    Iterator<RuleTuple> getRuleIterator(){
        return getAtom().getApplicableRules().stream()
                .flatMap(r -> {
                    r.rewriteToUserDefined(getAtom());
                    Unifier ruleUnifier = r.getUnifier(getAtom());
                    Unifier ruleUnifierInv = ruleUnifier.inverse();
                    return getAtom().getPermutationUnifiers(r.getHead().getAtom()).stream()
                            .map(permutationUnifier ->
                                    new RuleTuple(new InferenceRule(r)
                                            .propagateConstraints(getAtom(), permutationUnifier.combine(ruleUnifierInv)),
                                            ruleUnifier,
                                            permutationUnifier));
                })
                .sorted(Comparator.comparing(rt -> -rt.getRule().resolutionPriority()))
                .iterator();
    }

    /**
     *
     * <p>
     * Iterator for query answers maintaining the iterative behaviour of QSQ scheme.
     * </p>
     *
     * @author Kasper Piskorski
     *
     */
    private class QueryAnswerIterator extends ReasonerQueryIterator {

        private int iter = 0;
        private long answers = 0;
        private final Set<ReasonerAtomicQuery> subGoals = new HashSet<>();

        private final LazyQueryCache<ReasonerAtomicQuery> cache;
        private final LazyQueryCache<ReasonerAtomicQuery> dCache;
        private Iterator<Answer> answerIterator;

        QueryAnswerIterator(LazyQueryCache<ReasonerAtomicQuery> cache, LazyQueryCache<ReasonerAtomicQuery> dCache){
            this.cache = cache;
            this.dCache = dCache;
            this.answerIterator = query().answerStream(subGoals, cache, dCache, iter != 0).iterator();
        }

        private ReasonerAtomicQuery query(){ return ReasonerAtomicQuery.this;}

        /**
         * @return stream constructed out of the answer iterator
         */
        @Override
        public Stream<Answer> hasStream(){
            Iterable<Answer> iterable = () -> this;
            return StreamSupport.stream(iterable.spliterator(), false).distinct().peek(ans -> answers++);
        }

        private void computeNext(){
            iter++;
            subGoals.clear();
            answerIterator = query().answerStream(subGoals, cache, dCache, iter != 0).iterator();
        }

        /**
         * check whether answers available, if answers not fully computed compute more answers
         * @return true if answers available
         */
        @Override
        public boolean hasNext() {
            if (answerIterator.hasNext()) return true;
                //iter finished
            else {
                updateCache();
                long dAns = differentialAnswerSize();
                if (dAns != 0 || iter == 0) {
                    LOG.debug("Atom: " + query().getAtom() + " iter: " + iter + " answers: " + answers + " dAns = " + dAns);
                    computeNext();
                    return answerIterator.hasNext();
                }
                else return false;
            }
        }

        private void updateCache(){
            dCache.remove(cache);
            cache.add(dCache);
            cache.reload();
        }

        /**
         * @return single answer to the query
         */
        @Override
        public Answer next() {
            return answerIterator.next();
        }

        private long differentialAnswerSize(){
            return dCache.answerSize(subGoals);
        }
    }
}
