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
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationAtom;
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
import java.util.stream.Stream;

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
            priority = -RuleUtil.getDependentRules(getBody()).size();
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
        getBody().getAtoms(TypeAtom.class)
                .filter(t -> !t.isRelation())
                .forEach(atoms::add);
        return getBody().isEquivalent(ReasonerQueries.create(atoms, tx));
    }

    /**
     * rule requires materialisation in the context of resolving parent atom
     * if parent atom requires materialisation, head atom requires materialisation or if the head contains only fresh variables
     *
     * @return true if the rule needs to be materialised
     */
    public boolean requiresMaterialisation(Atom parentAtom) {
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

        //only transfer value predicates if head has a user specified value variable
        Atom headAtom = head.getAtom();
        Set<Atomic> bodyAtoms = new HashSet<>(body.getAtoms());
        if(headAtom.isResource() && ((ResourceAtom) headAtom).getMultiPredicate().isEmpty()){
            Set<ValuePredicate> vps  = Stream.concat(
                    parentAtom.getPredicates(ValuePredicate.class),
                    parentAtom.getInnerPredicates(ValuePredicate.class)
            )
                    .flatMap(vp -> vp.unify(unifier).stream())
                    .collect(toSet());
            headAtom = new ResourceAtom(
                    headAtom.getPattern().asVarPattern(),
                    headAtom.getPredicateVariable(),
                    ((ResourceAtom) headAtom).getRelationVariable(),
                    ((ResourceAtom) headAtom).getTypePredicate(),
                    vps,
                    headAtom.getParentQuery()
            );
            bodyAtoms.addAll(vps);
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
                            .filter(t -> ReasonerUtils.getSupers(t).contains(schemaConcept))
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

    private InferenceRule rewrite(){
        ReasonerAtomicQuery rewrittenHead = ReasonerQueries.atomic(head.getAtom().rewriteToUserDefined());
        List<Atom> bodyRewrites = new ArrayList<>();
        body.getAtoms(Atom.class)
                .map(at -> {
                    if (at.isRelation()
                            && !at.isUserDefined()
                            && Objects.equals(at.getSchemaConcept(), head.getAtom().getSchemaConcept())){
                        return at.rewriteToUserDefined();
                    } else {
                        return at;
                    }
                }).forEach(bodyRewrites::add);

        ReasonerQueryImpl rewrittenBody = ReasonerQueries.create(bodyRewrites, tx);
        return new InferenceRule(rewrittenHead, rewrittenBody, ruleId, tx);
    }

    /**
     * rewrite the rule to a form with user defined variables
     * @param parentAtom reference parent atom
     * @return rewritten rule
     */
    public InferenceRule rewriteToUserDefined(Atom parentAtom){
        return parentAtom.isUserDefined()? rewrite() : this;
    }

    /**
     * @param parentAtom atom to unify the rule with
     * @return corresponding unifier
     */
    public Unifier getUnifier(Atom parentAtom) {
        Atom childAtom = getRuleConclusionAtom();
        if (parentAtom.getSchemaConcept() != null){
            return childAtom.getUnifier(parentAtom);
        }
        //case of match all relation atom
        else{
            Atom extendedParent = ((RelationAtom) parentAtom)
                    .addType(childAtom.getSchemaConcept())
                    .inferTypes();
            return childAtom.getUnifier(extendedParent);
        }
    }
}
