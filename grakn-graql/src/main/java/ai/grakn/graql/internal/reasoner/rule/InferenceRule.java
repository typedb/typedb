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
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomBase;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.atom.binary.Resource;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueries;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.query.UnifierImpl;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import java.util.Map;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

    public InferenceRule(Rule rule, GraknGraph graph){
        ruleId = rule.getId();
        //TODO simplify once changes propagated to rule objects
        body = ReasonerQueries.create(conjunction(rule.getLHS().admin()), graph);
        head = ReasonerQueries.atomic(conjunction(rule.getRHS().admin()), graph);

        //run time check for head atom validity
        if (!getHead().getAtom().isAllowedToFormRuleHead()){
            throw new IllegalArgumentException(ErrorMessage.DISALLOWED_ATOM_IN_RULE_HEAD.getMessage(getHead().getAtom(), this.toString()));
        }
    }

    public InferenceRule(InferenceRule r){
        this.ruleId = r.getRuleId();
        this.body = ReasonerQueries.create(r.getBody());
        this.head = ReasonerQueries.atomic(r.getHead());
    }

    @Override
    public String toString(){
        return  "\n" + this.body.toString() + "->" + this.head.toString() + "\n";
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

    private static Conjunction<VarAdmin> conjunction(PatternAdmin pattern){
        Set<VarAdmin> vars = pattern
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
     * @param u rule unifier
     * @param pu permutation unifier
     * @return rule with propagated constraints from parent
     */
    public InferenceRule propagateConstraints(Atom parentAtom, Unifier u, Unifier pu){
        if (!parentAtom.isRelation() && !parentAtom.isResource()) return this;

        //only transfer value predicates if head has a user specified value variable
        Atom headAtom = head.getAtom();
        if(headAtom.isResource() && ((Resource) headAtom).getMultiPredicate().isEmpty()){
            Set<ValuePredicate> valuePredicates = parentAtom.getValuePredicates().stream()
                    .map(ValuePredicate::copy)
                    .map(type -> type.unify(pu))
                    .map(type -> type.unify(u))
                    .map(type -> (ValuePredicate) type)
                    .collect(toSet());
            head.addAtomConstraints(valuePredicates);
            body.addAtomConstraints(valuePredicates);
        }

        Set<TypeAtom> unifiedTypes = parentAtom.getTypeConstraints().stream()
                .map(TypeAtom::copy)
                .map(type -> type.unify(pu))
                .map(type -> type.unify(u))
                .map(type -> (TypeAtom) type)
                .collect(toSet());

        //remove less specific types if present
        Map<VarName, Type> unifiedVarTypeMap = unifiedTypes.stream()
                .collect(Collectors.toMap(AtomBase::getVarName, TypeAtom::getType));
        Set<VarName> unifiedTypeVars = unifiedTypes.stream()
                .map(Atom::getVarName)
                .collect(toSet());
        body.getTypeConstraints().stream()
                .filter(type -> unifiedTypeVars.contains(type.getVarName()))
                .filter(type -> !type.equals(unifiedVarTypeMap.get(type.getVarName())))
                .filter(type -> type.getType().subTypes().contains(unifiedVarTypeMap.get(type.getVarName())))
                .forEach(body::removeAtomic);

        body.addAtomConstraints(unifiedTypes.stream().filter(type -> !body.getTypeConstraints().contains(type)).collect(toSet()));
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
        body.getAtoms().stream()
                .filter(Atomic::isAtom).map(at -> (Atom) at)
                .filter(Atom::isRelation)
                .filter(at -> !at.isUserDefinedName())
                .filter(at -> Objects.nonNull(at.getType()))
                .filter(at -> at.getType().equals(head.getAtom().getType()))
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
        Unifier unifier = new UnifierImpl();
        if (parentAtom.getType() != null){
            unifier.merge(childAtom.getUnifier(parentAtom));
        }
        //case of match all relation atom
        else{
            Relation extendedParent = ((Relation) AtomicFactory
                    .create(parentAtom, parentAtom.getParentQuery()))
                    .addType(childAtom.getType());
            unifier.merge(childAtom.getUnifier(extendedParent));
        }
        return unifier;
    }
}
