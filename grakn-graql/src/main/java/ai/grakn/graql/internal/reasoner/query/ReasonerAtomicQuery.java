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
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.cache.Cache;
import ai.grakn.graql.internal.reasoner.cache.LazyQueryCache;
import ai.grakn.graql.internal.reasoner.iterator.LazyIterator;
import ai.grakn.graql.internal.reasoner.rule.InferenceRule;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

import static ai.grakn.graql.internal.reasoner.Utility.getListPermutations;
import static ai.grakn.graql.internal.reasoner.Utility.getUnifiersFromPermutations;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.entityTypeFilter;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.join;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.knownFilter;
import static ai.grakn.graql.internal.reasoner.query.QueryAnswerStream.nonEqualsFilter;
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
        atom = selectAtoms().iterator().next();
    }

    public ReasonerAtomicQuery(ReasonerAtomicQuery query) {
        super(query);
    }

    public ReasonerAtomicQuery(Atom at) {
        super(at);
        atom = selectAtoms().stream().findFirst().orElse(null);
    }

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
    public boolean addAtom(Atomic at) {
        if (super.addAtom(at)) {
            if (atom == null && at.isSelectable()) atom = (Atom) at;
            return true;
        } else return false;
    }

    @Override
    public boolean removeAtom(Atomic at) {
        if (super.removeAtom(at)) {
            if (atom != null & at.equals(atom)) atom = null;
            return true;
        } else return false;
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

    @Override
    public Map<VarName, VarName> getUnifiers(ReasonerQuery p){
        if (p == this) return new HashMap<>();
        ReasonerAtomicQuery parent = (ReasonerAtomicQuery) p;
        Map<VarName, VarName> unifiers = getAtom().getUnifiers(parent.getAtom());
        //get type unifiers
        Set<Atomic> unified = new HashSet<>();
        getAtom().getTypeConstraints().forEach(type -> {
            Set<Atomic> toUnify = Sets.difference(parent.getEquivalentAtoms(type), unified);
            Atomic equiv = toUnify.stream().findFirst().orElse(null);
            if (equiv != null){
                unifiers.putAll(type.getUnifiers(equiv));
                unified.add(equiv);
            }
        });
        return unifiers;
    }

    private LazyIterator<Map<VarName, Concept>> lazyLookup(Cache<ReasonerAtomicQuery, ?> cache) {
        boolean queryVisited = cache.contains(this);
        return queryVisited ? cache.getAnswerIterator(this) : lazyDBlookup(cache);
    }
    private LazyIterator<Map<VarName, Concept>> lazyDBlookup(Cache<ReasonerAtomicQuery, ?> cache) {
        Stream<Map<VarName, Concept>> dbStream = getMatchQuery().admin().streamWithVarNames();
        return cache.recordRetrieveLazy(this, dbStream);
    }

    /**
     * resolve the query by performing either a db or memory lookup, depending on which is more appropriate
     *
     * @param cache container of already performed query resolutions
     */
    public Stream<Map<VarName, Concept>> lookup(Cache<ReasonerAtomicQuery, ?> cache) {
        boolean queryVisited = cache.contains(this);
        return queryVisited ? cache.getAnswerStream(this) : DBlookup(cache);
    }

    /**
     * resolve the query by performing a db lookup with subsequent cache update
     */
    private Stream<Map<VarName, Concept>> DBlookup(Cache<ReasonerAtomicQuery, ?> cache) {
        Stream<Map<VarName, Concept>> dbStream = getMatchQuery().admin().streamWithVarNames();
        cache.record(this, dbStream);
        return cache.getAnswerStream(this);
    }

    /**
     * resolve the query by performing a db lookup
     */
    public Stream<Map<VarName, Concept>> DBlookup() {
        return getMatchQuery().admin().streamWithVarNames();
    }

    /**
     * execute insert on the query and return inserted answers
     */
    private Stream<Map<VarName, Concept>> insert() {
        InsertQuery insert = Graql.insert(getPattern().getVars()).withGraph(graph());
        return insert.stream()
                .map(m ->
                        m.entrySet().stream()
                                .collect(Collectors.toMap(k -> VarName.of(k.getKey()), Map.Entry::getValue)));
    }

    private Stream<Map<VarName, Concept>> materialiseDirect() {
        //extrapolate if needed
        if (atom.isRelation()) {
            Relation relAtom = (Relation) atom;
            Set<VarName> rolePlayers = relAtom.getRolePlayers();
            if (relAtom.getRoleVarTypeMap().size() != rolePlayers.size()) {
                RelationType relType = (RelationType) relAtom.getType();
                Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());
                Set<Map<VarName, Var>> roleMaps = new HashSet<>();
                Utility.computeRoleCombinations(rolePlayers, roles, new HashMap<>(), roleMaps);

                Stream<Map<VarName, Concept>> answerStream = Stream.empty();
                for (Map<VarName, Var> roleMap : roleMaps) {
                    Relation relationWithRoles = new Relation(relAtom.getVarName(), relAtom.getValueVariable(),
                            roleMap, relAtom.getPredicate(), this);
                    this.removeAtom(relAtom);
                    this.addAtom(relationWithRoles);
                    answerStream = Stream.concat(answerStream, insert());
                    this.removeAtom(relationWithRoles);
                    this.addAtom(relAtom);
                }
                return answerStream;
            } else {
                return insert();
            }
        } else {
            return insert();
        }
    }

    public Stream<Map<VarName, Concept>> materialise(Map<VarName, Concept> answer) {
        ReasonerAtomicQuery queryToMaterialise = new ReasonerAtomicQuery(this);
        Set<IdPredicate> subs = new HashSet<>();
        answer.forEach((var, con) -> subs.add(new IdPredicate(var, con, queryToMaterialise)));
        subs.forEach(queryToMaterialise::addAtom);
        return queryToMaterialise.materialiseDirect();
    }

    private Set<Map<VarName, VarName>> getPermutationUnifiers(Atom headAtom) {
        if (!(atom.isRelation() && headAtom.isRelation())) return new HashSet<>();
        List<VarName> permuteVars = new ArrayList<>();
        //if atom is match all atom, add type from rule head and find unmapped roles
        Relation relAtom = atom.getValueVariable().getValue().isEmpty() ?
                ((Relation) AtomicFactory.create(atom, atom.getParentQuery())).addType(headAtom.getType()) :
                (Relation) atom;
        relAtom.getUnmappedRolePlayers().forEach(permuteVars::add);

        List<List<VarName>> varPermutations = getListPermutations(new ArrayList<>(permuteVars)).stream()
                .filter(l -> !l.isEmpty()).collect(Collectors.toList());
        return getUnifiersFromPermutations(permuteVars, varPermutations);
    }

    private Stream<Map<VarName, Concept>> getIdPredicateAnswerStream(Stream<Map<VarName, Concept>> stream){
        Map<VarName, Concept> idPredicateAnswer = getIdPredicateAnswer();
        return stream.map(answer -> {
            answer.putAll(idPredicateAnswer);
            return answer;
        });
    }

    private Map<VarName, Concept> getIdPredicateAnswer(){
        return this.getTypeConstraints().stream()
                .map(TypeAtom::getPredicate).filter(Objects::nonNull)
                .collect(Collectors.toMap(IdPredicate::getVarName, sub -> graph().getConcept(sub.getPredicate())));
    }

    private Stream<Map<VarName, Concept>> fullJoin(List<ReasonerAtomicQuery> queries,
                                                   Set<ReasonerAtomicQuery> subGoals,
                                                   Cache<ReasonerAtomicQuery, ?> cache,
                                                   Cache<ReasonerAtomicQuery, ?> dCache,
                                                   boolean materialise){
        Iterator<ReasonerAtomicQuery> qit = queries.iterator();
        ReasonerAtomicQuery childAtomicQuery = qit.next();
        Stream<Map<VarName, Concept>> join = childAtomicQuery.answerStream(subGoals, cache, dCache, materialise, false);
        Set<VarName> joinedVars = childAtomicQuery.getVarNames();
        while(qit.hasNext()){
            childAtomicQuery = qit.next();
            Set<VarName> joinVars = Sets.intersection(joinedVars, childAtomicQuery.getVarNames());
            Stream<Map<VarName, Concept>> localSubs = childAtomicQuery.answerStream(subGoals, cache, dCache, materialise, false);
            join = join(join, localSubs, ImmutableSet.copyOf(joinVars));
            joinedVars.addAll(childAtomicQuery.getVarNames());
        }
        return join;
    }

    private Stream<Map<VarName, Concept>> differentialJoin(List<ReasonerAtomicQuery> queries,
                                                           Set<ReasonerAtomicQuery> subGoals,
                                                           Cache<ReasonerAtomicQuery, ?> cache,
                                                           Cache<ReasonerAtomicQuery, ?> dCache,
                                                           boolean materialise){
        Stream<Map<VarName, Concept>> join = Stream.empty();

        for(ReasonerAtomicQuery qi : queries){
            Stream<Map<VarName, Concept>> subs = qi.answerStream(subGoals, cache, dCache, materialise, true);
            Set<VarName> joinedVars = qi.getVarNames();
            for(ReasonerAtomicQuery qj : queries){
                if ( qj != qi ){
                    Set<VarName> joinVars = Sets.intersection(joinedVars, qj.getVarNames());
                    subs = join(subs, cache.getAnswerStream(qj), ImmutableSet.copyOf(joinVars));
                    joinedVars.addAll(qj.getVarNames());
                }
            }
            join = Stream.concat(join, subs);
        }
        return join.distinct();
    }

    private Stream<Map<VarName, Concept>> computeJoin(List<ReasonerAtomicQuery> queries,
                                                      Set<ReasonerAtomicQuery> subGoals,
                                                      Cache<ReasonerAtomicQuery, ?> cache,
                                                      Cache<ReasonerAtomicQuery, ?> dCache,
                                                      boolean materialise,
                                                      boolean differentialJoin) {
        if (differentialJoin){
            return differentialJoin(queries, subGoals, cache, dCache, materialise);
        } else {
            return fullJoin(queries, subGoals, cache, dCache, materialise);
        }
    }

    /**
     * attempt query resolution via application of a specific rule
     * @param rl rule through which to resolve the query
     * @param subGoals set of visited subqueries
     * @param cache collection of performed query resolutions
     * @param materialise materialisation flag
     * @return answers from rule resolution
     */
    private Stream<Map<VarName, Concept>> resolveViaRule(Rule rl,
                                                         Set<ReasonerAtomicQuery> subGoals,
                                                         Cache<ReasonerAtomicQuery, ?> cache,
                                                         Cache<ReasonerAtomicQuery, ?> dCache,
                                                         boolean materialise,
                                                         boolean differentialJoin){
        Atom atom = this.getAtom();
        InferenceRule rule = new InferenceRule(rl, graph());
        rule.unify(atom);
        ReasonerQueryImpl ruleBody = rule.getBody();
        ReasonerAtomicQuery ruleHead = rule.getHead();

        subGoals.add(this);
        Stream<Map<VarName, Concept>> subs = computeJoin(
                ruleBody.selectAtoms().stream().map(ReasonerAtomicQuery::new).collect(Collectors.toList()),
                subGoals,
                cache,
                dCache,
                materialise,
                differentialJoin);

        Stream<Map<VarName, Concept>> answers = subs
                .filter(a -> nonEqualsFilter(a, ruleBody.getFilters()))
                .flatMap(a -> varFilterFunction.apply(a, ruleHead.getVarNames()));

        if (materialise || ruleHead.getAtom().requiresMaterialisation()) {
            LazyIterator<Map<VarName, Concept>> known = ruleHead.lazyLookup(cache);
            LazyIterator<Map<VarName, Concept>> dknown = ruleHead.lazyLookup(dCache);
            Stream<Map<VarName, Concept>> newAnswers = answers.distinct()
                    .filter(a -> knownFilter(a, known.stream()))
                    .filter(a -> knownFilter(a, dknown.stream()))
                    .flatMap(ruleHead::materialise);

            answers = dCache.record(ruleHead, newAnswers)
                    .filter(a -> entityTypeFilter(a, atom.getMappedTypeConstraints()));
        }

        answers = getIdPredicateAnswerStream(answers)
                .flatMap(a -> varFilterFunction.apply(a, this.getVarNames()))
                .flatMap(a -> permuteFunction.apply(a, getPermutationUnifiers(ruleHead.getAtom())))
                .filter(a -> subFilter(a, atom.getUnmappedIdPredicates()))
                .filter(a -> entityTypeFilter(a, atom.getUnmappedTypeConstraints()));

        return dCache.record(this, answers);
    }

    /**
     * resolves the query by performing lookups and rule resolution and returns a stream of new answers
     * @param subGoals visited subGoals (recursive queries)
     * @param cache global query cache
     * @param dCache differential query cache
     * @param materialise whether inferred information should be materialised
     * @return stream of differential answers
     */
    public Stream<Map<VarName, Concept>> answerStream(Set<ReasonerAtomicQuery> subGoals,
                                                      Cache<ReasonerAtomicQuery, ?> cache,
                                                      Cache<ReasonerAtomicQuery, ?> dCache,
                                                      boolean materialise,
                                                      boolean differentialJoin){
        boolean queryAdmissible = !subGoals.contains(this);

        Stream<Map<VarName, Concept>> answerStream = cache.contains(this)? Stream.empty() : dCache.record(this, lookup(cache));
        if(queryAdmissible) {
            Set<Rule> rules = getAtom().getApplicableRules();
            Iterator<Rule> rIt = rules.iterator();
            while(rIt.hasNext()){
                Rule rule = rIt.next();
                Stream<Map<VarName, Concept>> localStream = resolveViaRule(rule, subGoals, cache, dCache, materialise, differentialJoin);
                answerStream = Stream.concat(answerStream, localStream);
            }
        }

        return dCache.record(this, answerStream);
    }

    @Override
    public Stream<Map<VarName, Concept>> resolve(boolean materialise) {
        if (!this.getAtom().isRuleResolvable()) {
            return this.getMatchQuery().admin().streamWithVarNames();
        } else {
            return new QueryAnswerIterator(materialise).hasStream();
        }
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
    private class QueryAnswerIterator implements Iterator<Map<VarName, Concept>> {

        final private QueryAnswers answers = new QueryAnswers();

        private int iter = 0;
        private final boolean materialise;
        private final Set<ReasonerAtomicQuery> subGoals = new HashSet<>();
        private final LazyQueryCache<ReasonerAtomicQuery> cache = new LazyQueryCache<>();
        private final LazyQueryCache<ReasonerAtomicQuery> dCache = new LazyQueryCache<>();
        private Iterator<Map<VarName, Concept>> answerIterator;

        public QueryAnswerIterator(boolean materialise){
            this.materialise = materialise;
            this.answerIterator = query().answerStream(subGoals, cache, dCache, materialise, iter != 0).iterator();
        }

        private ReasonerAtomicQuery query(){ return ReasonerAtomicQuery.this;}

        /**
         * @return stream constructed out of the answer iterator
         */
        Stream<Map<VarName, Concept>> hasStream(){
            Iterable<Map<VarName, Concept>> iterable = () -> this;
            return StreamSupport.stream(iterable.spliterator(), false).distinct();
        }

        private void computeNext(){
            iter++;
            subGoals.clear();
            answerIterator = query().answerStream(subGoals, cache, dCache, materialise, iter != 0).iterator();
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
                    LOG.debug("Atom: " + query().getAtom() + " iter: " + iter + " answers: " + answers.size() + " dAns = " + dAns);
                    computeNext();
                    return answerIterator.hasNext();
                }
                else return false;
            }
        }

        private void updateCache(){
            dCache.remove(cache, subGoals);
            cache.add(dCache);
            cache.reload();
        }

        /**
         * @return single answer to the query
         */
        @Override
        public Map<VarName, Concept> next() {
            Map<VarName, Concept> answer = answerIterator.next();
            answers.add(answer);
            return answer;
        }

        private long differentialAnswerSize(){
            return dCache.answerSize(subGoals);
        }
    }
}
