/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.internal.reasoner.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.MultiUnifier;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.internal.reasoner.ResolutionIterator;
import grakn.core.graql.internal.reasoner.atom.Atom;
import grakn.core.graql.internal.reasoner.atom.AtomicBase;
import grakn.core.graql.internal.reasoner.atom.AtomicFactory;
import grakn.core.graql.internal.reasoner.atom.NegatedAtomic;
import grakn.core.graql.internal.reasoner.atom.binary.IsaAtom;
import grakn.core.graql.internal.reasoner.atom.binary.IsaAtomBase;
import grakn.core.graql.internal.reasoner.atom.binary.RelationshipAtom;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.internal.reasoner.atom.predicate.NeqPredicate;
import grakn.core.graql.internal.reasoner.cache.Index;
import grakn.core.graql.internal.reasoner.cache.MultilevelSemanticCache;
import grakn.core.graql.internal.reasoner.explanation.JoinExplanation;
import grakn.core.graql.internal.reasoner.plan.ResolutionQueryPlan;
import grakn.core.graql.internal.reasoner.rule.InferenceRule;
import grakn.core.graql.internal.reasoner.rule.RuleUtils;
import grakn.core.graql.internal.reasoner.state.AnswerState;
import grakn.core.graql.internal.reasoner.state.ConjunctiveState;
import grakn.core.graql.internal.reasoner.state.CumulativeState;
import grakn.core.graql.internal.reasoner.state.NegatedConjunctiveState;
import grakn.core.graql.internal.reasoner.state.QueryStateBase;
import grakn.core.graql.internal.reasoner.state.ResolutionState;
import grakn.core.graql.internal.reasoner.unifier.UnifierType;
import grakn.core.graql.internal.reasoner.utils.Pair;
import grakn.core.graql.query.GetQuery;
import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.server.session.TransactionOLTP;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base reasoner query providing resolution and atom handling facilities for conjunctive graql queries.
 */
public class ReasonerQueryImpl implements ReasonerQuery {

    private final TransactionOLTP tx;
    private final ImmutableSet<Atomic> atomSet;
    private ConceptMap substitution = null;
    private ImmutableMap<Variable, Type> varTypeMap = null;

    ReasonerQueryImpl(Conjunction<Statement> pattern, TransactionOLTP tx) {
        this.tx = tx;
        this.atomSet = ImmutableSet.<Atomic>builder()
                .addAll(AtomicFactory.createAtoms(pattern, this).iterator())
                .build();

        if (!isNegationSafe()){
            throw new IllegalStateException("Negated pattern unsafe! Negated pattern variables not bound!");
        }
    }

    ReasonerQueryImpl(Set<Atomic> atoms, TransactionOLTP tx){
        this.tx = tx;
        this.atomSet = ImmutableSet.<Atomic>builder()
                .addAll(atoms.stream().map(at -> at.copy(this)).iterator())
                .build();
    }

    ReasonerQueryImpl(List<Atom> atoms, TransactionOLTP tx){
        this.tx = tx;
        this.atomSet =  ImmutableSet.<Atomic>builder()
                .addAll(atoms.stream()
                        .flatMap(at -> Stream.concat(Stream.of(at), at.getNonSelectableConstraints()))
                        .map(at -> at.copy(this)).iterator())
                .build();
    }

    ReasonerQueryImpl(Atom atom) {
        // TODO: This cast is unsafe - ReasonerQuery should return an EmbeeddedGraknTx
        this(Collections.singletonList(atom), (TransactionOLTP) /*TODO anything but this*/ atom.getParentQuery().tx());
    }

    ReasonerQueryImpl(ReasonerQueryImpl q) {
        this.tx = q.tx;
        this.atomSet =  ImmutableSet.<Atomic>builder()
                .addAll(q.getAtoms().stream().map(at -> at.copy(this)).iterator())
                .build();
    }

    @Override
    public ReasonerQuery conjunction(ReasonerQuery q) {
        return new ReasonerQueryImpl(
                Sets.union(getAtoms(), q.getAtoms()),
                this.tx()
        );
    }

    /**
     * @param sub substitution to be inserted into the query
     * @return corresponding query with additional substitution
     */
    public ReasonerQueryImpl withSubstitution(ConceptMap sub){
        return new ReasonerQueryImpl(Sets.union(this.getAtoms(), sub.toPredicates(this)), this.tx());
    }

    /**
     * @return corresponding reasoner query with inferred types
     */
    public ReasonerQueryImpl inferTypes() {
        return new ReasonerQueryImpl(getAtoms().stream().map(Atomic::inferTypes).collect(Collectors.toSet()), tx());
    }

    /**
     * @return corresponding positive query (with neq predicates removed)
     */
    public ReasonerQueryImpl positive(){
        return new ReasonerQueryImpl(getAtoms().stream().filter(at -> !(at instanceof NeqPredicate)).collect(Collectors.toSet()), tx());
    }

    /**
     * @param transform map defining id transform: var -> new id
     * @return new query with id predicates transformed according to the transform
     */
    public ReasonerQueryImpl transformIds(Map<Variable, ConceptId> transform){
        Set<Atomic> atoms = this.getAtoms(IdPredicate.class).map(p -> {
            ConceptId conceptId = transform.get(p.getVarName());
            if (conceptId != null) return IdPredicate.create(p.getVarName(), conceptId, p.getParentQuery());
            return p;
        }).collect(Collectors.toSet());
        getAtoms().stream().filter(at -> !(at instanceof IdPredicate)).forEach(atoms::add);
        return new ReasonerQueryImpl(atoms, tx());
    }

    @Override
    public String toString(){
        return "{\n\t" +
                getAtoms(Atomic.class).map(Atomic::toString).collect(Collectors.joining(";\n\t")) +
                "\n}";
    }

    public ReasonerQuery copy() {
        return new ReasonerQueryImpl(this);
    }

    //alpha-equivalence equality
    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
        ReasonerQueryImpl q2 = (ReasonerQueryImpl) obj;
        return this.isEquivalent(q2);
    }

    @Override
    public int hashCode() {
        return ReasonerQueryEquivalence.AlphaEquivalence.hash(this);
    }

    /**
     * @param q query to be compared with
     * @return true if two queries are alpha-equivalent
     */
    public boolean isEquivalent(ReasonerQueryImpl q) {
        return ReasonerQueryEquivalence.AlphaEquivalence.equivalent(this, q);
    }

    @Override
    public TransactionOLTP tx() {
        return tx;
    }

    @Override
    public void checkValid() { getAtoms().forEach(Atomic::checkValid);}

    public Conjunction<Pattern> getPattern() {
        return Pattern.and(
                getAtoms().stream()
                        .map(Atomic::getCombinedPattern)
                        .flatMap(p -> p.statements().stream())
                        .collect(Collectors.toSet())
        );
    }

    @Override
    public Set<String> validateOntologically() {
        return getAtoms().stream()
                .flatMap(at -> at.validateOntologically().stream())
                .collect(Collectors.toSet());
    }

    private boolean isNegationSafe(){
        Set<NegatedAtomic> negated = getAtoms(NegatedAtomic.class).collect(Collectors.toSet());
        Set<Variable> negatedVars = negated.stream().flatMap(at -> at.getVarNames().stream()).collect(Collectors.toSet());
        Set<Variable> boundVars = getAtoms().stream().filter(at -> !negated.contains(at)).flatMap(at -> at.getVarNames().stream()).collect(Collectors.toSet());

        negated.stream().map(NegatedAtomic::inner).filter(Atomic::isAtom).map(Atom.class::cast).flatMap(Atom::getInnerPredicates).flatMap(at -> at.getVarNames().stream()).forEach(boundVars::add);
        getAtoms(Atom.class).flatMap(Atom::getInnerPredicates).flatMap(at -> at.getVarNames().stream()).forEach(boundVars::add);
        return boundVars.containsAll(negatedVars);
    }

    @Override
    public boolean isRuleResolvable() {
        return selectAtoms().anyMatch(Atom::isRuleResolvable);
    }

    @Override
    public boolean isPositive() { return getAtoms().stream().allMatch(Atomic::isPositive); }

    /**
     * @return true if this query contains disconnected atoms that are unbounded
     */
    public boolean isBoundlesslyDisconnected(){
        return !isAtomic()
                && selectAtoms()
                .filter(at -> !at.isBounded())
                .anyMatch(Atom::isDisconnected);
    }

    /**
     * @return true if the query requires direct schema lookups
     */
    public boolean requiresSchema(){ return selectAtoms().anyMatch(Atom::requiresSchema);}

    /**
     * @return true if this query is atomic
     */
    public boolean isAtomic() {
        return atomSet.stream().filter(Atomic::isSelectable).count() == 1;
    }

    /**
     * @param typedVar variable of interest
     * @param parentType to be checked
     * @return true if typing the typeVar with type is compatible with role configuration of this query
     */
    @Override
    public boolean isTypeRoleCompatible(Variable typedVar, Type parentType){
        if (parentType == null || Schema.MetaSchema.isMetaLabel(parentType.label())) return true;

        Set<Type> parentTypes = parentType.subs().collect(Collectors.toSet());
        return getAtoms(RelationshipAtom.class)
                .filter(ra -> ra.getVarNames().contains(typedVar))
                .noneMatch(ra -> ra.getRoleVarMap().entries().stream()
                        //get roles this type needs to play
                        .filter(e -> e.getValue().equals(typedVar))
                        .filter(e -> !Schema.MetaSchema.isMetaLabel(e.getKey().label()))
                        //check if it can play it
                        .anyMatch(e -> e.getKey().players().noneMatch(parentTypes::contains)));
    }

    @Override
    public Set<Atomic> getAtoms() { return atomSet;}

    @Override
    public <T extends Atomic> Stream<T> getAtoms(Class<T> type) {
        return getAtoms().stream().filter(type::isInstance).map(type::cast);
    }

    @Override
    public Set<Variable> getVarNames() {
        Set<Variable> vars = new HashSet<>();
        getAtoms().forEach(atom -> vars.addAll(atom.getVarNames()));
        return vars;
    }

    @Override
    public MultiUnifier getMultiUnifier(ReasonerQuery parent) {
        return getMultiUnifier(parent, UnifierType.EXACT);
    }

    /**
     * @param parent query we want to unify this query with
     * @param unifierType unifier type
     * @return corresponding multiunifier
     */
    public MultiUnifier getMultiUnifier(ReasonerQuery parent, UnifierType unifierType){
        throw GraqlQueryException.getUnifierOfNonAtomicQuery();
    }

    public GetQuery getQuery() {
        return Graql.match(getPattern()).get();
    }

    private Stream<IsaAtom> inferEntityTypes(ConceptMap sub) {
        Set<Variable> typedVars = getAtoms(IsaAtomBase.class).map(AtomicBase::getVarName).collect(Collectors.toSet());
        return Stream.concat(
                getAtoms(IdPredicate.class),
                sub.toPredicates(this).stream().map(IdPredicate.class::cast)
        )
                .filter(p -> !typedVars.contains(p.getVarName()))
                .map(p -> new Pair<>(p, tx().<Concept>getConcept(p.getPredicate())))
                .filter(p -> Objects.nonNull(p.getValue()))
                .filter(p -> p.getValue().isEntity())
                .map(p -> IsaAtom.create(p.getKey().getVarName(), new Variable(), p.getValue().asEntity().type(), false,this));
    }

    private Map<Variable, Type> getVarTypeMap(Stream<IsaAtomBase> isas){
        HashMap<Variable, Type> map = new HashMap<>();
        isas
                .map(at -> new Pair<>(at.getVarName(), at.getSchemaConcept()))
                .filter(p -> Objects.nonNull(p.getValue()))
                .filter(p -> p.getValue().isType())
                .forEach(p -> {
                    Variable var = p.getKey();
                    Type newType = p.getValue().asType();
                    Type type = map.get(var);
                    if (type == null) map.put(var, newType);
                    else {
                        boolean isSubType = type.subs().anyMatch(t -> t.equals(newType));
                        if (isSubType) map.put(var, newType);
                    }
                });
        return map;
    }

    @Override
    public ImmutableMap<Variable, Type> getVarTypeMap() {
        if (varTypeMap == null) {
            this.varTypeMap = getVarTypeMap(new ConceptMap());
        }
        return varTypeMap;
    }

    public ImmutableMap<Variable, Type> getVarTypeMap(boolean inferTypes) {
        Set<IsaAtomBase> isas = getAtoms(IsaAtomBase.class).collect(Collectors.toSet());
        return ImmutableMap.copyOf(
                getVarTypeMap()
                        .entrySet().stream()
                        .filter(e -> inferTypes ||
                                isas.stream()
                                        .filter(isa -> isa.getVarName().equals(e.getKey()))
                                        .filter(isa -> Objects.nonNull(isa.getSchemaConcept()))
                                        .anyMatch(isa -> isa.getSchemaConcept().equals(e.getValue()))
                        )
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
    }

    @Override
    public ImmutableMap<Variable, Type> getVarTypeMap(ConceptMap sub) {
        return ImmutableMap.copyOf(
                getVarTypeMap(
                        Stream.concat(
                                getAtoms(IsaAtomBase.class),
                                inferEntityTypes(sub)
                        )
                )
        );
    }

    /**
     * @param var variable name
     * @return id predicate for the specified var name if any
     */
    @Nullable
    private IdPredicate getIdPredicate(Variable var) {
        return getAtoms(IdPredicate.class)
                .filter(sub -> sub.getVarName().equals(var))
                .findFirst().orElse(null);
    }

    /**
     * returns id transform that would convert this query to a query alpha-equivalent to the query,
     * provided they are structurally equivalent
     * @param query for which the transform is to be constructed
     * @param unifier between this query and provided query
     * @return id transform
     */
    public Map<Variable, ConceptId> idTransform(ReasonerQueryImpl query, Unifier unifier){
        Map<Variable, ConceptId> transform = new HashMap<>();
        this.getAtoms(IdPredicate.class)
                .forEach(thisP -> {
                    Collection<Variable> vars = unifier.get(thisP.getVarName());
                    Variable var = !vars.isEmpty()? Iterators.getOnlyElement(vars.iterator()) : thisP.getVarName();
                    IdPredicate p2 = query.getIdPredicate(var);
                    if ( p2 != null) transform.put(thisP.getVarName(), p2.getPredicate());
                });
        return transform;
    }

    /**
     * @return selected atoms
     */
    public Stream<Atom> selectAtoms() {
        return getAtoms(Atom.class).filter(Atomic::isSelectable);
    }

    /** Does id predicates -> answer conversion
     * @return substitution obtained from all id predicates (including internal) in the query
     */
    public ConceptMap getSubstitution(){
        if (substitution == null) {
            Set<Variable> varNames = getVarNames();
            Set<IdPredicate> predicates = getAtoms(IsaAtomBase.class)
                    .map(IsaAtomBase::getTypePredicate)
                    .filter(Objects::nonNull)
                    .filter(p -> varNames.contains(p.getVarName()))
                    .collect(Collectors.toSet());
            getAtoms(IdPredicate.class).forEach(predicates::add);

            HashMap<Variable, Concept> answerMap = new HashMap<>();
            predicates.forEach(p -> {
                Concept concept = tx().getConcept(p.getPredicate());
                if (concept == null) throw GraqlQueryException.idNotFound(p.getPredicate());
                answerMap.put(p.getVarName(), concept);
            });
            substitution = new ConceptMap(answerMap);
        }
        return substitution;
    }

    public ConceptMap getRoleSubstitution(){
        Map<Variable, Concept> roleSub = new HashMap<>();
        getAtoms(RelationshipAtom.class)
                .flatMap(RelationshipAtom::getRolePredicates)
                .forEach(p -> {
                    Concept concept = tx().getConcept(p.getPredicate());
                    if (concept == null) throw GraqlQueryException.idNotFound(p.getPredicate());
                    roleSub.put(p.getVarName(), concept);
                });
        return new ConceptMap(roleSub);
    }

    /**
     * @return true if this query is a ground query
     */
    public boolean isGround(){
        return getSubstitution().vars().containsAll(getVarNames());
    }

    /**
     * @return true if this query requires atom decomposition
     */
    public boolean requiresDecomposition(){
        return this.selectAtoms().anyMatch(Atom::requiresDecomposition);
    }
    /**
     * @return rewritten (decomposed) version of the query
     */
    public ReasonerQueryImpl rewrite(){
        if (!requiresDecomposition()) return this;
        return new ReasonerQueryImpl(
                this.selectAtoms()
                        .flatMap(at -> at.rewriteToAtoms().stream())
                        .collect(Collectors.toList()),
                tx()
        );
    }

    @Override
    public Stream<ConceptMap> resolve() {
        return resolve(new MultilevelSemanticCache());
    }

    public Stream<ConceptMap> resolve(MultilevelSemanticCache cache){
        return new ResolutionIterator(this, cache).hasStream();
    }

    /**
     * @param sub partial substitution
     * @param u unifier with parent state
     * @param parent parent state
     * @param subGoals set of visited sub goals
     * @param cache query cache
     * @return resolution subGoal formed from this query
     */
    public ResolutionState subGoal(ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache){
        ConjunctiveState conjunctiveState = new ConjunctiveState(this, sub, u, parent, subGoals, cache);
        return isPositive()? conjunctiveState : new NegatedConjunctiveState(conjunctiveState);
    }

    /**
     * @param sub partial substitution
     * @param u unifier with parent state
     * @param parent parent state
     * @param subGoals set of visited sub goals
     * @param cache query cache
     * @return resolution subGoals formed from this query obtained by expanding the inferred types contained in the query
     */
    public Stream<ResolutionState> subGoals(ConceptMap sub, Unifier u, QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache){
        return getQueryStream(sub)
                .map(q -> q.subGoal(sub, u, parent, subGoals, cache));
    }

    /**
     * @return stream of queries obtained by inserting all inferred possible types (if ambiguous)
     */
    Stream<ReasonerQueryImpl> getQueryStream(ConceptMap sub){
        return Stream.of(this);
    }

    /**
     * @param parent parent state
     * @param subGoals set of visited sub goals
     * @param cache query cache
     * @return query state iterator (db iter + unifier + state iter) for this query
     */
    public Iterator<ResolutionState> queryStateIterator(QueryStateBase parent, Set<ReasonerAtomicQuery> subGoals, MultilevelSemanticCache cache){
        Iterator<AnswerState> dbIterator;
        Iterator<QueryStateBase> subGoalIterator;

        if(!this.isRuleResolvable()) {
            dbIterator = tx.stream(getQuery(), false)
                    .map(ans -> ans.explain(new JoinExplanation(this, ans)))
                    .map(ans -> new AnswerState(ans, parent.getUnifier(), parent))
                    .iterator();
            subGoalIterator = Collections.emptyIterator();
        } else {
            dbIterator = Collections.emptyIterator();

            ResolutionQueryPlan queryPlan = new ResolutionQueryPlan(this);
            subGoalIterator = Iterators.singletonIterator(new CumulativeState(queryPlan.queries(), new ConceptMap(), parent.getUnifier(), parent, subGoals, cache));
        }
        return Iterators.concat(dbIterator, subGoalIterator);
    }


    /**
     * reiteration might be required if rule graph contains loops with negative flux
     * or there exists a rule which head satisfies body
     * @return true if because of the rule graph form, the resolution of this query may require reiteration
     */
    public boolean requiresReiteration() {
        Set<InferenceRule> dependentRules = RuleUtils.getDependentRules(this);
        return RuleUtils.subGraphIsCyclical(dependentRules)||
            RuleUtils.subGraphHasRulesWithHeadSatisfyingBody(dependentRules);
    }

    /**
     * @return set o variables containing a matching substitution
     */
    private Set<Variable> subbedVars(){
        return getAtoms(IdPredicate.class).map(Atomic::getVarName).collect(Collectors.toSet());
    }

    /**
     * @return answer index corresponding to corresponding partial substitution
     */
    public ConceptMap getAnswerIndex(){
        return getSubstitution().project(subbedVars());
    }

    /**
     * @return var index consisting of variables with a substitution
     */
    public Index index(){
        return Index.of(subbedVars());
    }
}
