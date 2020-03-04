/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.graql.reasoner.rule;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.util.ConceptUtils;
import grakn.core.graql.reasoner.atom.Atom;
import grakn.core.graql.reasoner.atom.binary.AttributeAtom;
import grakn.core.graql.reasoner.atom.binary.RelationAtom;
import grakn.core.graql.reasoner.atom.binary.TypeAtom;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.query.ReasonerAtomicQuery;
import grakn.core.graql.reasoner.query.ReasonerQueryFactory;
import grakn.core.graql.reasoner.query.ReasonerQueryImpl;
import grakn.core.graql.reasoner.query.ResolvableQuery;
import grakn.core.graql.reasoner.state.AnswerPropagatorState;
import grakn.core.graql.reasoner.state.ResolutionState;
import grakn.core.graql.reasoner.state.RuleState;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.graql.reasoner.atom.Atomic;
import grakn.core.kb.graql.reasoner.unifier.MultiUnifier;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Class providing resolution and higher level facilities for Rule objects.
 * </p>
 *
 *
 */
public class InferenceRule {

    private final Rule rule;
    private final ReasonerQueryFactory reasonerQueryFactory;
    private final ResolvableQuery body;
    private final ReasonerAtomicQuery head;

    private long priority = Long.MAX_VALUE;
    private Atom conclusionAtom = null;
    private Boolean requiresMaterialisation = null;

    public InferenceRule(Rule rule, ReasonerQueryFactory reasonerQueryFactory){
        this.rule = rule;
        this.reasonerQueryFactory = reasonerQueryFactory;
        //TODO simplify once changes propagated to rule objects
        this.body = reasonerQueryFactory.resolvable(Iterables.getOnlyElement(rule.when().getNegationDNF().getPatterns()));
        this.head = reasonerQueryFactory.atomic(conjunction(rule.then()));
    }

    private InferenceRule(ReasonerAtomicQuery head, ResolvableQuery body, Rule rule, ReasonerQueryFactory reasonerQueryFactory){
        this.rule = rule;
        this.head = head;
        this.body = body;
        this.reasonerQueryFactory = reasonerQueryFactory;
    }

    @Override
    public String toString(){
        return  "\n" + this.body.toString() + "->\n" + this.head.toString() + "]\n";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || this.getClass() != obj.getClass()) return false;
        InferenceRule rule = (InferenceRule) obj;
        return this.getBody().equals(rule.getBody())
                && this.getHead().equals(rule.getHead());
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 37 + getBody().hashCode();
        hashCode = hashCode * 37 + getHead().hashCode();
        return hashCode;
    }

    /**
     * @return the priority with which the rule should be fired
     */
    long resolutionPriority(){
        if (priority == Long.MAX_VALUE) {
            //NB: this has to be relatively lightweight as it is called on each rule
            //TODO come with a more useful metric
            boolean bodyRuleResolvable = getBody().getAtoms(Atom.class)
                    .map(Atom::getSchemaConcept)
                    .filter(Objects::nonNull)
                    .map(Concept::asType)
                    .anyMatch(t -> t.thenRules().findFirst().isPresent());
            priority = bodyRuleResolvable? -1 : 0;
            //resolve base types first
            priority -= getHead().getAtom().getSchemaConcept().sups().count();
        }
        return priority;
    }

    private Conjunction<Statement> conjunction(Pattern pattern){
        Set<Statement> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Graql.and(vars);
    }

    public Rule getRule(){ return rule;}

    /**
     * @return true if the rule has disconnected head, i.e. head and body do not share any variables
     */
    private boolean hasDisconnectedHead(){
        return Sets.intersection(body.getVarNames(), head.getVarNames()).isEmpty();
    }

    /**
     * @return true if head satisfies the pattern specified in the body of the rule
     */
    boolean headSatisfiesBody(){
        if (!getBody().isAtomic()) return false;
        Set<Atomic> atoms = new HashSet<>(getHead().getAtoms());
        Set<Variable> headVars = getHead().getVarNames();
        getBody().getAtoms(TypeAtom.class)
                .filter(t -> !t.isRelation())
                .filter(t -> !Sets.intersection(t.getVarNames(), headVars).isEmpty())
                .forEach(atoms::add);
        return reasonerQueryFactory.create(atoms).isEquivalent(getBody());
    }
    
    /**
     * rule requires materialisation in the context of resolving parent atom
     * if parent atom requires materialisation, head atom requires materialisation or if the head contains only fresh variables
     *
     * @return true if the rule needs to be materialised
     */
    public boolean requiresMaterialisation(Atom parentAtom){
        if (requiresMaterialisation == null) {
            requiresMaterialisation = parentAtom.requiresMaterialisation()
                    || getHead().getAtom().requiresMaterialisation()
                    || hasDisconnectedHead();
        }
        return requiresMaterialisation;
    }

    /**
     * @return body of the rule of the form head :- body
     */
    public ResolvableQuery getBody(){ return body;}

    /**
     * @return head of the rule of the form head :- body
     */
    public ReasonerAtomicQuery getHead(){ return head;}

    /**
     * @return reasoner query formed of combining head and body queries
     */
    private ReasonerQueryImpl getCombinedQuery(){
        Set<Atomic> allAtoms = new HashSet<>(body.getAtoms());
        //NB: if rule acts as a sub, do not include type overlap
        boolean subHead = head.getAtom().isType();
        if (subHead){
            body.getAtoms().stream()
                    .filter(Atomic::isType)
                    .filter(at -> at.getVarName().equals(head.getAtom().getVarName()))
                    .forEach(allAtoms::remove);
        }
        allAtoms.add(head.getAtom());
        return reasonerQueryFactory.create(allAtoms);
    }

    /**
     * @return a conclusion atom which parent contains all atoms in the rule
     */
    public Atom getRuleConclusionAtom() {
        if (conclusionAtom == null) {
            conclusionAtom = getCombinedQuery().getAtoms(Atom.class)
                    .filter(at -> at.equals(head.getAtom()))
                    .findFirst().orElse(null);
        }
        return conclusionAtom;
    }

    /**
     * @param parentAtom atom containing constraints (parent)
     * @param unifier unifier unifying parent with the rule
     * @return rule with propagated constraints from parent
     */
    private InferenceRule propagateConstraints(Atom parentAtom, Unifier unifier){
        if (!parentAtom.isRelation() && !parentAtom.isAttribute()) return this;
        Atom headAtom = head.getAtom();

        //we are only rewriting the conjunction atoms (not complement atoms) as
        //the constraints are propagated from the conjunctive part anyway and
        //all variables in the -ve part not referenced in the +ve part have a different scope
        ReasonerQueryImpl bodyConjunction = getBody().asComposite().getConjunctiveQuery();
        Set<Atomic> bodyConjunctionAtoms = new HashSet<>(bodyConjunction.getAtoms());

        //transfer value predicates
        Set<Variable> bodyVars = bodyConjunction.getVarNames();
        Set<ValuePredicate> vpsToPropagate = parentAtom.getPredicates(ValuePredicate.class)
                .flatMap(vp -> vp.unify(unifier).stream())
                .filter(vp -> bodyVars.contains(vp.getVarName()))
                .collect(toSet());
        bodyConjunctionAtoms.addAll(vpsToPropagate);

        //if head is a resource merge vps into head
        if (headAtom.isAttribute()) {
            AttributeAtom resourceHead = (AttributeAtom) headAtom;

            if (resourceHead.getMultiPredicate().isEmpty()) {
                Set<ValuePredicate> innerVps = parentAtom.getInnerPredicates(ValuePredicate.class)
                        .flatMap(vp -> vp.unify(unifier).stream())
                        .collect(toSet());
                bodyConjunctionAtoms.addAll(innerVps);

                // TODO revert this to old implementation of instantiating without copy constructor
                // or do it properly with a factory
                headAtom = AttributeAtom.create(resourceHead.getPattern(), resourceHead.getAttributeVariable(),
                        resourceHead.getRelationVariable(),
                        resourceHead.getPredicateVariable(),
                        resourceHead.getTypeLabel(),
                        innerVps,
                        resourceHead.getParentQuery(),
                        resourceHead.context());
                //headAtom = resourceHead.copy(innerVps);
            }
        }

        Set<TypeAtom> unifiedTypes = parentAtom.getTypeConstraints()
                .flatMap(type -> type.unify(unifier).stream())
                .collect(toSet());

        //set rule body types to sub types of combined query+rule types
        Set<TypeAtom> ruleTypes = bodyConjunction.getAtoms(TypeAtom.class).filter(t -> !t.isRelation()).collect(toSet());
        Set<TypeAtom> allTypes = Sets.union(unifiedTypes, ruleTypes);
        allTypes.stream()
                .filter(ta -> {
                    SchemaConcept schemaConcept = ta.getSchemaConcept();
                    SchemaConcept subType = allTypes.stream()
                            .map(Atom::getSchemaConcept)
                            .filter(Objects::nonNull)
                            .filter(t -> ConceptUtils.nonMetaSups(t).contains(schemaConcept))
                            .findFirst().orElse(null);
                    return schemaConcept == null || subType == null;
                }).forEach(t -> bodyConjunctionAtoms.add(t.copy(body)));

        ReasonerQueryImpl rewrittenBodyConj = reasonerQueryFactory.create(bodyConjunctionAtoms);
        ResolvableQuery rewrittenBody = getBody().isComposite() ?
                reasonerQueryFactory.composite(rewrittenBodyConj, getBody().asComposite().getComplementQueries()) :
                rewrittenBodyConj;
        return new InferenceRule(
                reasonerQueryFactory.atomic(headAtom),
                rewrittenBody,
                rule,
                reasonerQueryFactory
        );
    }

    /**
     * @return true if the application of the rule results in type redefinition
     */
    public boolean redefinesType(){
        Variable instanceVariable = getHead().getAtom().getVarName();
        return getBody().getVarNames().contains(instanceVariable);
    }

    /**
     * @return true if the application of the rule results in addition of roleplayers to existing relations
     */
    public boolean appendsRolePlayers(){
        Atom headAtom = getHead().getAtom();
        SchemaConcept headType = headAtom.getSchemaConcept();
        if (headType.isRelationType()
                && headAtom.getVarName().isReturned()) {
            RelationAtom bodyAtom = getBody().getAtoms(RelationAtom.class)
                    .filter(at -> Objects.nonNull(at.getSchemaConcept()))
                    .filter(at -> at.getSchemaConcept().equals(headType))
                    .filter(at -> at.getVarName().isReturned())
                    .findFirst().orElse(null);
            return bodyAtom != null;
        }
        return false;
    }

    private InferenceRule rewriteHeadToRelation(Atom parentAtom){
        if (parentAtom.isRelation() && getHead().getAtom().isAttribute()){
            return new InferenceRule(
                    reasonerQueryFactory.atomic(getHead().getAtom().toRelationAtom()),
                    getBody(),
                    rule,
                    reasonerQueryFactory
            );
        }
        return this;
    }

    private InferenceRule rewriteVariables(Atom parentAtom){
        if (parentAtom.isUserDefined() || parentAtom.requiresRoleExpansion()) {
            //NB we don't have to rewrite complements as we don't allow recursion atm
            return new InferenceRule(
                    reasonerQueryFactory.atomic(getHead().getAtom().rewriteToUserDefined(parentAtom)),
                    getBody(),
                    rule,
                    reasonerQueryFactory
            );
        }
        return this;
    }

    private InferenceRule rewriteBodyAtoms(){
        if (getBody().requiresDecomposition()) {
            return new InferenceRule(getHead(), getBody().rewrite(), rule, reasonerQueryFactory);
        }
        return this;
    }

    /**
     * rewrite the rule to a form with user defined variables
     * @param parentAtom reference parent atom
     * @return rewritten rule
     */
    public InferenceRule rewrite(Atom parentAtom){
        return this
                .rewriteBodyAtoms()
                .rewriteHeadToRelation(parentAtom)
                .rewriteVariables(parentAtom);
    }

    /**
     * @param parentAtom atom to unify the rule with
     * @return corresponding unifier
     */
    public MultiUnifier getMultiUnifier(Atom parentAtom) {
        Atom childAtom = getRuleConclusionAtom();
        if (parentAtom.getSchemaConcept() != null){
            return childAtom.getMultiUnifier(parentAtom, UnifierType.RULE);
        }
        //case of match all atom (atom without type)
        else{
            Atom extendedParent = parentAtom
                    .addType(childAtom.getSchemaConcept())
                    .inferTypes();
            return childAtom.getMultiUnifier(extendedParent, UnifierType.RULE);
        }
    }

    /**
     * @param parentAtom atom to which this rule is applied
     * @param ruleUnifier unifier with parent state
     * @param parent parent state
     * @param visitedSubGoals set of visited sub goals
     * @return resolution resolutionState formed from this rule
     */
    public ResolutionState subGoal(Atom parentAtom, Unifier ruleUnifier, AnswerPropagatorState parent, Set<ReasonerAtomicQuery> visitedSubGoals){
        Unifier ruleUnifierInverse = ruleUnifier.inverse();

        //delta' = theta . thetaP . delta
        ConceptMap partialSubPrime = ruleUnifierInverse.apply(
                parentAtom.getParentQuery().getSubstitution()
        );

        return new RuleState(this.propagateConstraints(parentAtom, ruleUnifierInverse), partialSubPrime, ruleUnifier, parent, visitedSubGoals);
    }

}
