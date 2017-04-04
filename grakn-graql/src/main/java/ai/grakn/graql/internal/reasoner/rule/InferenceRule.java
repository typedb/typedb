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
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.AtomicFactory;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import ai.grakn.graql.internal.reasoner.query.UnifierImpl;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import javafx.util.Pair;

import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Class providing resolution and higher level facilities for rule objects.
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
        body = new ReasonerQueryImpl(conjunction(rule.getLHS().admin()), graph);
        head = new ReasonerAtomicQuery(conjunction(rule.getRHS().admin()), graph);

        //if head query is a relation query, require roles to be specified
        if (head.getAtom().isRelation()){
            Relation headAtom = (Relation) head.getAtom();
            if (headAtom.getRoleVarTypeMap().keySet().size() < headAtom.getRelationPlayers().size()) {
                throw new IllegalArgumentException(ErrorMessage.HEAD_ROLES_MISSING.getMessage(this.toString()));
            }
        }
    }

    public InferenceRule(InferenceRule r){
        this.ruleId = r.getRuleId();
        this.body = new ReasonerQueryImpl(r.getBody());
        this.head = new ReasonerAtomicQuery(r.getHead());
    }

    @Override
    public String toString(){
        return  "\n" + this.body.toString() + "\n->\n" + this.head.toString() + "\n";
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

    /**rule needs to be materialised if head atom requires materialisation or if its head contains only fresh variables
     * @return true if the rule needs to be materialised
     */
    public boolean requiresMaterialisation(){
        return getHead().getAtom().requiresMaterialisation()
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
        ReasonerQueryImpl ruleQuery = new ReasonerQueryImpl(head);
        Atom atom = ruleQuery.selectAtoms().iterator().next();
        body.getAtoms().forEach(at -> ruleQuery.addAtomic(at.copy()));
        return atom;
    }

    /**
     * @param parentAtom from which constrained are propagated
     * @return the inference rule with constraints
     */
    public  InferenceRule propagateConstraints(Atom parentAtom){
        if (!parentAtom.isRelation() && !parentAtom.isResource()) return this;
        Set<Predicate> predicates = parentAtom.getPredicates().stream()
                .collect(toSet());
        Set<Atom> types = parentAtom.getTypeConstraints().stream()
                .collect(toSet());

        head.addAtomConstraints(predicates);
        body.addAtomConstraints(predicates);
        body.addAtomConstraints(types.stream().filter(type -> !body.containsEquivalentAtom(type)).collect(toSet()));
        return this;
    }

    private void rewriteHead(Atom parentAtom){
        Atom childAtom = head.getAtom();
        Pair<Atom, Unifier> rewrite = childAtom.rewriteToUserDefinedWithUnifiers();
        Unifier rewriteUnifiers = rewrite.getValue();
        Atom newAtom = rewrite.getKey();
        if (newAtom != childAtom){
            head.removeAtomic(childAtom);
            head.addAtomic(newAtom);
            body.unify(rewriteUnifiers);

            //resolve captures
            Set<VarName> varIntersection = Sets.intersection(body.getVarNames(), parentAtom.getVarNames());
            varIntersection = Sets.difference(varIntersection, rewriteUnifiers.keySet());
            varIntersection.forEach(var -> body.unify(var, VarName.anon()));
        }
    }

    private void rewriteBody(){
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
    }

    private InferenceRule unifyViaAtom(Atom parentAtom) {
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
        return this.unify(unifier);
    }

    /**
     * @param unifier to be applied on this rule
     * @return unified rule
     */
    public InferenceRule unify(Unifier unifier){
        //do alpha-conversion
        head.unify(unifier);
        body.unify(unifier);
        return this;
    }

    /**
     * make rule consistent variable-wise with the parent atom by means of unification
     * @param parentAtom atom the rule should be unified with
     */
    public InferenceRule unify(Atom parentAtom) {
        if (parentAtom.isUserDefinedName()) rewriteHead(parentAtom);
        unifyViaAtom(parentAtom);
        if (head.getAtom().isUserDefinedName()) rewriteBody();
        return this;
    }
}
