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
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.AnswerExplanation;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.cache.Cache;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.internal.reasoner.explanation.LookupExplanation;
import ai.grakn.graql.internal.reasoner.explanation.RuleExplanation;
import ai.grakn.graql.internal.reasoner.cache.QueryCache;
import ai.grakn.graql.internal.reasoner.iterator.ReasonerQueryIterator;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import java.util.Collections;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static ai.grakn.graql.internal.reasoner.Utility.getListPermutations;
import static ai.grakn.graql.internal.reasoner.Utility.getUnifiersFromPermutations;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.entityTypeFilter;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.knownFilterWithInverse;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.permuteFunction;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.subFilter;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.varFilterFunction;

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
    private static final Logger LOG = LoggerFactory.getLogger(ReasonerAtomicQuery.class);

    public ReasonerAtomicQuery(Conjunction<VarAdmin> pattern, GraknGraph graph) {
        super(pattern, graph);
        atom = selectAtoms().stream().findFirst().orElse(null);
    }

    public ReasonerAtomicQuery(ReasonerAtomicQuery query) {
        super(query);
        atom = selectAtoms().stream().findFirst().orElse(null);
    }

    public ReasonerAtomicQuery(Atom at) {
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
    public int hashCode() {
        return super.hashCode() + 37;
    }

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
    public void unify(Unifier unifier) {
        super.unify(unifier);
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

    @Override
    public Unifier getUnifier(ReasonerQuery p){
        if (p == this) return new UnifierImpl();
        ReasonerAtomicQuery parent = (ReasonerAtomicQuery) p;
        Unifier unifier = getAtom().getUnifier(parent.getAtom());
        //get type unifiers
        Set<Atomic> unified = new HashSet<>();
        getAtom().getTypeConstraints().forEach(type -> {
            Set<Atomic> toUnify = Sets.difference(parent.getEquivalentAtoms(type), unified);
            Atomic equiv = toUnify.stream().findFirst().orElse(null);
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
     *
     * @param cache container of already performed query resolutions
     */
    public Stream<Answer> lookup(Cache<ReasonerAtomicQuery, ?> cache) {
        boolean queryVisited = cache.contains(this);
        return queryVisited ? cache.getAnswerStream(this) : DBlookup(cache);
    }

    /**
     * resolve the query by performing a db lookup with subsequent cache update
     */
    private Stream<Answer> DBlookup(Cache<ReasonerAtomicQuery, ?> cache) {
        AnswerExplanation exp = new LookupExplanation(this);
        Stream<Answer> dbStream = getMatchQuery().admin().streamWithVarNames()
                .map(QueryAnswer::new)
                .map(a -> a.explain(exp));
        return cache.record(this, dbStream);
    }

    /**
     * resolve the query by performing a db lookup
     */
    public Stream<Answer> DBlookup() {
        AnswerExplanation exp = new LookupExplanation(this);
        return getMatchQuery().admin().streamWithVarNames()
                .map(QueryAnswer::new)
                .map(a -> a.explain(exp));
    }

    /**
     * execute insert on the query and return inserted answers
     */
    private Stream<Answer> insert() {
        InsertQuery insert = Graql.insert(getPattern().getVars()).withGraph(graph());
        return insert.stream()
                .map(m -> m.entrySet().stream().collect(Collectors.toMap(k -> VarName.of(k.getKey()), Map.Entry::getValue)))
                .map(QueryAnswer::new);
    }

    private Stream<Answer> materialiseDirect() {
        //extrapolate if needed
        if (atom.isRelation()) {
            Relation relAtom = (Relation) atom;
            Set<VarName> rolePlayers = relAtom.getRolePlayers();
            if (relAtom.getRoleVarTypeMap().size() != rolePlayers.size()) {
                RelationType relType = (RelationType) relAtom.getType();
                Set<RoleType> roles = Sets.newHashSet(relType.relates());
                Set<Map<VarName, Var>> roleMaps = new HashSet<>();
                Utility.computeRoleCombinations(rolePlayers, roles, new HashMap<>(), roleMaps);

                Stream<Answer> answerStream = Stream.empty();
                for (Map<VarName, Var> roleMap : roleMaps) {
                    Relation relationWithRoles = new Relation(relAtom.getVarName(), relAtom.getValueVariable(),
                            roleMap, relAtom.getPredicate(), this);
                    this.removeAtomic(relAtom);
                    this.addAtomic(relationWithRoles);
                    answerStream = Stream.concat(answerStream, insert());
                    this.removeAtomic(relationWithRoles);
                    this.addAtomic(relAtom);
                }
                return answerStream;
            } else {
                return insert();
            }
        } else {
            return insert();
        }
    }

    public Stream<Answer> materialise(Answer answer) {
        ReasonerAtomicQuery queryToMaterialise = new ReasonerAtomicQuery(this);
        queryToMaterialise.addSubstitution(answer);
        return queryToMaterialise.materialiseDirect()
                .map(ans -> ans.setExplanation(answer.getExplanation()));
    }

    private Set<Unifier> getPermutationUnifiers(Atom headAtom) {
        if (!(atom.isRelation() && headAtom.isRelation())) return new HashSet<>();

        //if atom is match all atom, add type from rule head and find unmapped roles
        Relation relAtom = atom.getValueVariable().getValue().isEmpty() ?
                ((Relation) AtomicFactory.create(atom, atom.getParentQuery())).addType(headAtom.getType()) :
                (Relation) atom;
        List<VarName> permuteVars = new ArrayList<>(relAtom.getUnmappedRolePlayers());

        List<List<VarName>> varPermutations = getListPermutations(new ArrayList<>(permuteVars)).stream()
                .filter(l -> !l.isEmpty()).collect(Collectors.toList());
        return getUnifiersFromPermutations(permuteVars, varPermutations);
    }

    @SuppressWarnings("unchecked")
    //Query type atoms -> Answer
    private Answer getIdPredicateAnswer(){
        Object result = this.getTypeConstraints().stream()
                .map(TypeAtom::getPredicate).filter(Objects::nonNull)
                .collect(Collectors.toMap(IdPredicate::getVarName, sub -> graph().getConcept(sub.getPredicate())));
        return new QueryAnswer((Map<VarName, Concept>)result);
    }

    private Stream<Answer> getIdPredicateAnswerStream(Stream<Answer> stream){
        Answer idPredicateAnswer = getIdPredicateAnswer();
        return stream.map(answer -> {
            AnswerExplanation exp = answer.getExplanation();
            return answer.merge(idPredicateAnswer).explain(exp);
        });
    }

    private Stream<Answer> getFilteredAnswerStream(Stream<Answer> answers, ReasonerAtomicQuery ruleHead){
        Set<VarName> vars = getVarNames();
        Set<Unifier> permutationUnifiers = getPermutationUnifiers(ruleHead.getAtom());
        Set<IdPredicate> unmappedIdPredicates = atom.getUnmappedIdPredicates();
        Set<TypeAtom> mappedTypeConstraints = atom.getMappedTypeConstraints();
        Set<TypeAtom> unmappedTypeConstraints = atom.getUnmappedTypeConstraints();
        return getIdPredicateAnswerStream(answers)
                .filter(a -> entityTypeFilter(a, mappedTypeConstraints))
                .flatMap(a -> varFilterFunction.apply(a, vars))
                .flatMap(a -> permuteFunction.apply(a, permutationUnifiers))
                .filter(a -> subFilter(a, unmappedIdPredicates))
                .filter(a -> entityTypeFilter(a, unmappedTypeConstraints));
    }

    /**
     * attempt query resolution via application of a specific rule
     * @param rule rule to apply to resolve the query
     * @param subGoals set of visited subqueries
     * @param cache collection of performed query resolutions
     * @param materialise materialisation flag
     * @return answers from rule resolution
     */
    private Stream<Answer> resolveViaRule(InferenceRule rule,
                                          Set<ReasonerAtomicQuery> subGoals,
                                          Cache<ReasonerAtomicQuery, ?> cache,
                                          Cache<ReasonerAtomicQuery, ?> dCache,
                                          boolean materialise,
                                          boolean explanation,
                                          boolean differentialJoin){
        Atom atom = this.getAtom();
        rule.unify(atom).propagateConstraints(atom);

        ReasonerQueryImpl ruleBody = rule.getBody();
        ReasonerAtomicQuery ruleHead = rule.getHead();
        Set<VarName> varsToRetain = rule.hasDisconnectedHead()? ruleBody.getVarNames() : ruleHead.getVarNames();

        subGoals.add(this);
        Stream<Answer> answers = ruleBody
                .computeJoin(subGoals, cache, dCache, materialise, explanation, differentialJoin)
                .flatMap(a -> varFilterFunction.apply(a, varsToRetain))
                .distinct()
                .map(ans -> ans.explain(new RuleExplanation(rule)));

        if (materialise || rule.requiresMaterialisation()) {
            if (!cache.contains(ruleHead)) ruleHead.lookup(cache);
            //filter known to make sure no duplicates are inserted (put behaviour)
            Map<Pair<VarName, Concept>, Set<Answer>> known = cache.getInverseAnswerMap(ruleHead);
            Map<Pair<VarName, Concept>, Set<Answer>> dknown = dCache.getInverseAnswerMap(ruleHead);

            answers = answers
                    .filter(a -> knownFilterWithInverse(a, known))
                    .filter(a -> knownFilterWithInverse(a, dknown))
                    .flatMap(ruleHead::materialise);

            answers = dCache.record(ruleHead, answers);
        }
        //if query not exactly equal to the rule head, do some conversion
        return this.equals(ruleHead)? dCache.record(ruleHead, answers) : dCache.record(this, getFilteredAnswerStream(answers, ruleHead));
    }

    /**
     * resolves the query by performing lookups and rule resolution and returns a stream of new answers
     * @param subGoals visited subGoals (recursive queries)
     * @param cache global query cache
     * @param dCache differential query cache
     * @param materialise whether inferred information should be materialised
     * @return stream of differential answers
     */
    public Stream<Answer> answerStream(Set<ReasonerAtomicQuery> subGoals,
                                       Cache<ReasonerAtomicQuery, ?> cache,
                                       Cache<ReasonerAtomicQuery, ?> dCache,
                                       boolean materialise,
                                       boolean explanation,
                                       boolean differentialJoin){
        boolean queryAdmissible = !subGoals.contains(this);

        Stream<Answer> answerStream = cache.contains(this)? Stream.empty() : dCache.record(this, lookup(cache));
        if(queryAdmissible) {
            Set<InferenceRule> rules = getAtom().getApplicableRules();
            for (InferenceRule rule : rules) {
                Stream<Answer> localStream = resolveViaRule(rule, subGoals, cache, dCache, materialise, explanation, differentialJoin);
                answerStream = Stream.concat(answerStream, localStream);
            }
        }

        return dCache.record(this, answerStream);
    }

    public Stream<Answer> resolve(boolean materialise, boolean explanation, LazyQueryCache<ReasonerAtomicQuery> cache, LazyQueryCache<ReasonerAtomicQuery> dCache) {
        if (!this.getAtom().isRuleResolvable()) {
            return this.getMatchQuery().admin().streamWithVarNames().map(QueryAnswer::new);
        } else {
            return new QueryAnswerIterator(materialise, explanation, cache, dCache).hasStream();
        }
    }


    public ReasonerQueryIterator iterator(Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> cache){
        //TODO switch to iterative deepening for queries with no subs?
        return new ReasonerAtomicQueryIterator(subGoals, cache);
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
        private final boolean materialise;
        private final boolean explanation;
        private final Set<ReasonerAtomicQuery> subGoals = new HashSet<>();

        private final LazyQueryCache<ReasonerAtomicQuery> cache;
        private final LazyQueryCache<ReasonerAtomicQuery> dCache;
        private Iterator<Answer> answerIterator;

        QueryAnswerIterator(boolean materialise, boolean explanation, LazyQueryCache<ReasonerAtomicQuery> cache, LazyQueryCache<ReasonerAtomicQuery> dCache){
            this.materialise = materialise;
            this.explanation = explanation;
            this.cache = cache;
            this.dCache = dCache;
            this.answerIterator = query().answerStream(subGoals, cache, dCache, materialise, explanation, iter != 0).iterator();
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
            answerIterator = query().answerStream(subGoals, cache, dCache, materialise, explanation, iter != 0).iterator();
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

    /**
     * Tuple-at-a-time iterator for this atomic query.
     * Resolves the atomic query by:
     * 1) doing DB lookup
     * 2) applying a rule
     * 3) doing a lemma (previously derived answer) lookup
     */
    private class ReasonerAtomicQueryIterator extends ReasonerQueryIterator {

        private final QueryCache<ReasonerAtomicQuery> cache;
        private final Set<ReasonerAtomicQuery> subGoals;
        private final Iterator<InferenceRule> ruleIterator;
        private Iterator<Answer> queryIterator = Collections.emptyIterator();

        private InferenceRule currentRule = null;

        ReasonerAtomicQueryIterator(Set<ReasonerAtomicQuery> subGoals, QueryCache<ReasonerAtomicQuery> qc){
            this.subGoals = subGoals;
            this.cache = qc;

            boolean hasFullSubstitution = hasFullSubstitution();
            this.queryIterator = lookup(cache).iterator();

            //if this already has full substitution and exists in the db then do not resolve further
            if(subGoals.contains(ReasonerAtomicQuery.this)
                || (hasFullSubstitution && queryIterator.hasNext() ) ){
                this.ruleIterator = Collections.emptyIterator();
            }
            else {
                this.ruleIterator = getRuleIterator();
            }

            //mark as visited and hence not admissible
            if (ruleIterator.hasNext()) subGoals.add(ReasonerAtomicQuery.this);
        }

        private Iterator<InferenceRule> getRuleIterator(){
            Atom atom = getAtom();
            //list cause rules with permuted role types are alpha-equivalent
            List<InferenceRule> rules = getAtom().getApplicableRules().stream()
                    .map(rule -> rule.unify(atom))
                    .collect(Collectors.toList());
            if (atom.isRelation()
                    && !((Relation)atom).getUnmappedRolePlayers().isEmpty()) {
                rules = rules.stream()
                        .flatMap(rule -> {
                            Set<Unifier> permutationUnifiers = getPermutationUnifiers(rule.getHead().getAtom());
                            return permutationUnifiers.stream()
                                    .map(unifier -> new InferenceRule(rule).unify(unifier));
                        }).collect(Collectors.toList());
            }
            return rules.stream().map(rule -> rule.propagateConstraints(getAtom())).iterator();
        }

        @Override
        public boolean hasNext() {
            if (queryIterator.hasNext()) return true;
            else{
                if (ruleIterator.hasNext()) {
                    currentRule = ruleIterator.next();
                    queryIterator = currentRule.getBody().iterator(new QueryAnswer(), subGoals, cache);
                    return hasNext();
                }
                else return false;
            }
        }

        @Override
        public Answer next() {
            Answer sub = queryIterator.next()
                    .merge(getIdPredicateAnswer())
                    .filterVars(getVarNames());
            if (currentRule != null) sub = sub.explain(new RuleExplanation(currentRule));
            return cache.recordAnswer(ReasonerAtomicQuery.this, sub);
        }

    }
}
