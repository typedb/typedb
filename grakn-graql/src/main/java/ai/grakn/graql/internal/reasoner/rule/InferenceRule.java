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

package ai.grakn.graql.internal.reasoner.rule;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.MultiUnifier;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.UnifierType;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.binary.ResourceAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Class providing resolution and higher level facilities for {@link Rule} objects.
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class InferenceRule {

    private final GraknTx tx;
    private final ConceptId ruleId;
    private final ReasonerQueryImpl body;
    private final ReasonerAtomicQuery head;

    private int priority = Integer.MAX_VALUE;
    private Boolean requiresMaterialisation = null;

    public InferenceRule(Rule rule, GraknTx tx){
        this.tx = tx;
        this.ruleId = rule.getId();
        //TODO simplify once changes propagated to rule objects
        this.body = ReasonerQueries.create(conjunction(rule.getWhen().admin()), tx);
        this.head = ReasonerQueries.atomic(conjunction(rule.getThen().admin()), tx);
    }

    private InferenceRule(ReasonerAtomicQuery head, ReasonerQueryImpl body, ConceptId ruleId, GraknTx tx){
        this.tx = tx;
        this.ruleId = ruleId;
        this.head = head;
        this.body = body;
    }

    public InferenceRule(InferenceRule r){
        this.tx = r.tx;
        this.ruleId = r.getRuleId();
        this.body = ReasonerQueries.create(r.getBody());
        this.head = ReasonerQueries.atomic(r.getHead());
    }

    @Override
    public String toString(){
        return  "\n" + this.body.toString() + "->\n" + this.head.toString() + "[" + resolutionPriority() +"]\n";
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
    public int resolutionPriority(){
        if (priority == Integer.MAX_VALUE) {
            priority = -RuleUtils.getDependentRules(getBody()).size();
        }
        return priority;
    }

    private Conjunction<VarPatternAdmin> conjunction(PatternAdmin pattern){
        Set<VarPatternAdmin> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    public ConceptId getRuleId(){ return ruleId;}

    /**
     * @return true if the rule has disconnected head, i.e. head and body do not share any variables
     */
    public boolean hasDisconnectedHead(){
        return Sets.intersection(body.getVarNames(), head.getVarNames()).isEmpty();
    }

    /**
     * @return true if head satisfies the pattern specified in the body of the rule
     */
    boolean headSatisfiesBody(){
        Set<Atomic> atoms = new HashSet<>(getHead().getAtoms());
        Set<Var> headVars = getHead().getVarNames();
        getBody().getAtoms(TypeAtom.class)
                .filter(t -> !t.isRelation())
                .filter(t -> !Sets.intersection(t.getVarNames(), headVars).isEmpty())
                .forEach(atoms::add);
        return getBody().isEquivalent(ReasonerQueries.create(atoms, tx));
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
    public ReasonerQueryImpl getBody(){ return body;}

    /**
     * @return head of the rule of the form head :- body
     */
    public ReasonerAtomicQuery getHead(){ return head;}

    /**
     * @param sub substitution to be added to the rule
     * @return inference rule with added substitution
     */
    public InferenceRule withSubstitution(Answer sub){
        return new InferenceRule(
                ReasonerQueries.atomic(getHead(), sub),
                ReasonerQueries.create(getBody(), sub),
                ruleId,
                tx
        );
    }

    /**
     * @return reasoner query formed of combining head and body queries
     */
    private ReasonerQueryImpl getCombinedQuery(){
        Set<Atomic> allAtoms = new HashSet<>();
        allAtoms.add(head.getAtom());
        body.getAtoms().forEach(allAtoms::add);
        return ReasonerQueries.create(allAtoms, tx);
    }

    /**
     * @return a conclusion atom which parent contains all atoms in the rule
     */
    public Atom getRuleConclusionAtom() {
        return getCombinedQuery().getAtoms(Atom.class).filter(at -> at.equals(head.getAtom())).findFirst().orElse(null);
    }

    /**
     * @param parentAtom atom containing constraints (parent)
     * @param unifier unifier unifying parent with the rule
     * @return rule with propagated constraints from parent
     */
    public InferenceRule propagateConstraints(Atom parentAtom, Unifier unifier){
        if (!parentAtom.isRelation() && !parentAtom.isResource()) return this;
        Atom headAtom = head.getAtom();
        Set<Atomic> bodyAtoms = new HashSet<>(body.getAtoms());

        //transfer value predicates
        parentAtom.getPredicates(ValuePredicate.class)
                .flatMap(vp -> vp.unify(unifier).stream())
                .forEach(bodyAtoms::add);

        //if head is a resource merge vps into head
        if (headAtom.isResource() && ((ResourceAtom) headAtom).getMultiPredicate().isEmpty()) {
            ResourceAtom resourceHead = (ResourceAtom) headAtom;
            Set<ValuePredicate> innerVps = parentAtom.getInnerPredicates(ValuePredicate.class)
                    .flatMap(vp -> vp.unify(unifier).stream())
                    .peek(bodyAtoms::add)
                    .collect(toSet());
            headAtom = new ResourceAtom(
                    headAtom.getPattern().asVarPattern(),
                    headAtom.getPredicateVariable(),
                    resourceHead.getRelationVariable(),
                    resourceHead.getTypePredicate(),
                    innerVps,
                    headAtom.getParentQuery()
            );
        }

        Set<TypeAtom> unifiedTypes = parentAtom.getTypeConstraints()
                .flatMap(type -> type.unify(unifier).stream())
                .collect(toSet());

        //set rule body types to sub types of combined query+rule types
        Set<TypeAtom> ruleTypes = body.getAtoms(TypeAtom.class).filter(t -> !t.isRelation()).collect(toSet());
        Set<TypeAtom> allTypes = Sets.union(unifiedTypes, ruleTypes);
        allTypes.stream()
                .filter(ta -> {
                    SchemaConcept schemaConcept = ta.getSchemaConcept();
                    SchemaConcept subType = allTypes.stream()
                            .map(Atom::getSchemaConcept)
                            .filter(Objects::nonNull)
                            .filter(t -> ReasonerUtils.supers(t).contains(schemaConcept))
                            .findFirst().orElse(null);
                    return schemaConcept == null || subType == null;
                }).forEach(t -> bodyAtoms.add(AtomicFactory.create(t, body)));
        return new InferenceRule(
                ReasonerQueries.atomic(headAtom),
                ReasonerQueries.create(bodyAtoms, tx),
                ruleId,
                tx
        );
    }

    private InferenceRule rewrite(Atom parentAtom){
        ReasonerAtomicQuery rewrittenHead = ReasonerQueries.atomic(head.getAtom().rewriteToUserDefined(parentAtom));
        List<Atom> bodyRewrites = new ArrayList<>();
        body.getAtoms(Atom.class)
                .map(at -> {
                    if (at.isRelation()
                            && !at.isUserDefined()
                            && Objects.equals(at.getSchemaConcept(), head.getAtom().getSchemaConcept())){
                        return at.rewriteToUserDefined(parentAtom);
                    } else {
                        return at;
                    }
                }).forEach(bodyRewrites::add);

        ReasonerQueryImpl rewrittenBody = ReasonerQueries.create(bodyRewrites, tx);
        return new InferenceRule(rewrittenHead, rewrittenBody, ruleId, tx);
    }

    private boolean requiresRewrite(Atom parentAtom){
        return parentAtom.isUserDefined() || parentAtom.requiresRoleExpansion();
    }

    /**
     * rewrite the rule to a form with user defined variables
     * @param parentAtom reference parent atom
     * @return rewritten rule
     */
    public InferenceRule rewriteToUserDefined(Atom parentAtom){
        return requiresRewrite(parentAtom)? rewrite(parentAtom) : this;
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
}
