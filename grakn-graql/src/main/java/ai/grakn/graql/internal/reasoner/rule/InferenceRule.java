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

import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.Rule;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.utils.ReasonerUtils;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.binary.RelationAtom;
import ai.grakn.graql.internal.reasoner.atom.binary.ResourceAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import com.google.common.collect.Sets;

import java.util.HashSet;
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

    private final ConceptId ruleId;
    private final ReasonerQueryImpl body;
    private final ReasonerAtomicQuery head;

    private int priority = Integer.MAX_VALUE;

    public InferenceRule(Rule rule, GraknGraph graph){
        ruleId = rule.getId();
        //TODO simplify once changes propagated to rule objects
        body = ReasonerQueries.create(conjunction(rule.getWhen().admin()), graph);
        head = ReasonerQueries.atomic(conjunction(rule.getThen().admin()), graph);
    }

    public InferenceRule(InferenceRule r){
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
            priority = getBody().resolutionPriority();
        }
        return priority;
    }

    private static Conjunction<VarPatternAdmin> conjunction(PatternAdmin pattern){
        Set<VarPatternAdmin> vars = pattern
                .getDisjunctiveNormalForm().getPatterns()
                .stream().flatMap(p -> p.getPatterns().stream()).collect(toSet());
        return Patterns.conjunction(vars);
    }

    public ConceptId getRuleId(){ return ruleId;}

    /**
     * @return true if head and body do not share any variables
     */
    public boolean hasDisconnectedHead(){
        return Sets.intersection(body.getVarNames(), head.getVarNames()).isEmpty();
    }

    /**
     * rule requires materialisation in the context of resolving parentatom
     * if parent atom requires materialisation, head atom requires materialisation or if the head contains only fresh variables
     *
     * @return true if the rule needs to be materialised
     */
    public boolean requiresMaterialisation(Atom parentAtom){
        return parentAtom.requiresMaterialisation()
            || getHead().getAtom().requiresMaterialisation()
            || hasDisconnectedHead();}

    /**
     * @return body of the rule of the form head :- body
     */
    public ReasonerQueryImpl getBody(){ return body;}

    /**
     * @return head of the rule of the form head :- body
     */
    public ReasonerAtomicQuery getHead(){ return head;}

    /**
     * @return a conclusion atom which parent contains all atoms in the rule
     */
    public Atom getRuleConclusionAtom() {
        ReasonerAtomicQuery ruleQuery = ReasonerQueries.atomic(head);
        Atom atom = ruleQuery.getAtom();
        body.getAtoms().forEach(at -> ruleQuery.addAtomic(at.copy()));
        return atom;
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
        if(headAtom.isResource() && ((ResourceAtom) headAtom).getMultiPredicate().isEmpty()){
            Set<ValuePredicate> valuePredicates = parentAtom.getValuePredicates().stream()
                    .flatMap(vp -> vp.unify(unifier).stream())
                    .collect(toSet());
            head.addAtomConstraints(valuePredicates);
            body.addAtomConstraints(valuePredicates);
        }

        Set<TypeAtom> unifiedTypes = parentAtom.getTypeConstraints().stream()
                .flatMap(type -> type.unify(unifier).stream())
                .collect(toSet());

        //set rule body types to sub types of combined query+rule types
        Set<TypeAtom> ruleTypes = body.getTypeConstraints().stream().filter(t -> !t.isRelation()).collect(toSet());
        Set<TypeAtom> allTypes = Sets.union(unifiedTypes, ruleTypes);
        Set<TypeAtom> types = allTypes.stream()
                .filter(ta -> {
                    OntologyConcept ontologyConcept = ta.getOntologyConcept();
                    OntologyConcept subType = allTypes.stream()
                            .map(Atom::getOntologyConcept)
                            .filter(Objects::nonNull)
                            .filter(t -> ReasonerUtils.getSupers(t).contains(ontologyConcept))
                            .findFirst().orElse(null);
                    return ontologyConcept == null || subType == null;
                }).collect(toSet());

        ruleTypes.forEach(body::removeAtomic);
        body.addAtomConstraints(types);

        return this;
    }

    private InferenceRule rewriteHead(){
        Atom childAtom = head.getAtom();
        Atom newAtom = childAtom.rewriteToUserDefined();
        head.removeAtomic(childAtom);
        head.addAtomic(newAtom);
        return this;
    }

    private InferenceRule rewriteBody(){
        new HashSet<>(body.getAtoms()).stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(Atom::isRelation)
                .filter(at -> !at.isUserDefinedName())
                .filter(at -> Objects.nonNull(at.getOntologyConcept()))
                .filter(at -> at.getOntologyConcept().equals(head.getAtom().getOntologyConcept()))
                .forEach(at -> {
                    Atom rewrite = at.rewriteToUserDefined();
                    body.removeAtomic(at);
                    body.addAtomic(rewrite);
                    });
        return this;
    }

    /**
     * rewrite the rule to a form with user defined variables
     * @param parentAtom reference parent atom
     * @return rewritten rule
     */
    public InferenceRule rewriteToUserDefined(Atom parentAtom){
        return parentAtom.isUserDefinedName()? this.rewriteHead().rewriteBody() : this;
    }

    /**
     * @param parentAtom atom to unify the rule with
     * @return corresponding unifier
     */
    public Unifier getUnifier(Atom parentAtom) {
        Atom childAtom = getRuleConclusionAtom();
        if (parentAtom.getOntologyConcept() != null){
            return childAtom.getUnifier(parentAtom);
        }
        //case of match all relation atom
        else{
            Atom extendedParent = ((RelationAtom) parentAtom)
                    .addType(childAtom.getOntologyConcept())
                    .inferTypes();
            return childAtom.getUnifier(extendedParent);
        }
    }
}
