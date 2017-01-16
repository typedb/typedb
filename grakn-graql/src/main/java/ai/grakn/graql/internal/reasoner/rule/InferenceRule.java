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
import ai.grakn.concept.Rule;
import ai.grakn.graql.VarName;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.atom.binary.Resource;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.ReasonerAtomicQuery;
import ai.grakn.graql.internal.reasoner.query.ReasonerQueryImpl;
import java.util.HashSet;
import javafx.util.Pair;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.match;

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

    private final ReasonerQueryImpl body;
    private final ReasonerAtomicQuery head;

    public InferenceRule(Rule rule, GraknGraph graph){
        body = new ReasonerQueryImpl(match(rule.getLHS()), graph);
        head = new ReasonerAtomicQuery(match(rule.getRHS()), graph);
    }

    /**
     * @return body of the rule of the form head :- body
     */
    public ReasonerQueryImpl getBody(){return body;}

    /**
     * @return head of the rule of the form head :- body
     */
    public ReasonerAtomicQuery getHead(){return head;}

    /**
     * @return a conclusion atom which parent contains all atoms in the rule
     */
    public Atom getRuleConclusionAtom() {
        ReasonerQueryImpl ruleQuery = new ReasonerQueryImpl(head);
        Atom atom = ruleQuery.selectAtoms().iterator().next();
        body.getAtoms().forEach(at -> ruleQuery.addAtom(at.clone()));
        return atom;
    }

    private void propagateConstraints(Atom parentAtom){
        if(parentAtom.isRelation() || parentAtom.isResource()) {
            Set<Atom> types = parentAtom.getTypeConstraints().stream()
                    .filter(type -> !body.containsEquivalentAtom(type))
                    .collect(Collectors.toSet());
            //get all predicates apart from those that correspond to role players with ambiguous role types
            Set<VarName> unmappedVars = parentAtom.isRelation()? ((Relation) parentAtom).getUnmappedRolePlayers() : new HashSet<>();
            Set<Predicate> predicates = parentAtom.getPredicates().stream()
                    .filter(pred -> !unmappedVars.contains(pred.getVarName()))
                    .collect(Collectors.toSet());

            head.addAtomConstraints(predicates);
            body.addAtomConstraints(predicates);
            body.addAtomConstraints(types);
        }
        head.selectAppend(parentAtom.getParentQuery().getSelectedNames());
    }

    private void rewriteHead(Atom parentAtom){
        Atom childAtom = head.getAtom();
        Pair<Atom, Map<VarName, VarName>> rewrite = childAtom.rewrite(parentAtom, head);
        Map<VarName, VarName> rewriteUnifiers = rewrite.getValue();
        Atom newAtom = rewrite.getKey();
        if (newAtom != childAtom){
            head.removeAtom(childAtom);
            head.addAtom(newAtom);
            unify(rewriteUnifiers);

            //resolve captures
            Set<VarName> varIntersection = body.getVarSet();
            varIntersection.retainAll(parentAtom.getVarNames());
            varIntersection.removeAll(rewriteUnifiers.keySet());
            varIntersection.forEach(var -> body.unify(var, Patterns.varName()));
        }
    }

    private void unify(Map<VarName, VarName> unifiers){
        //do alpha-conversion
        head.unify(unifiers);
        body.unify(unifiers);
    }

    private void unifyViaAtom(Atom parentAtom) {
        Atomic childAtom = getRuleConclusionAtom();
        Map<VarName, VarName> unifiers = childAtom.getUnifiers(parentAtom);
        unify(unifiers);
    }

    /**
     * make rule consistent variable-wise with the parent atom by means of unification
     * @param parentAtom atom the rule should be unified with
     */
   public void unify(Atom parentAtom) {
        rewriteHead(parentAtom);
        unifyViaAtom(parentAtom);
        propagateConstraints(parentAtom);
    }
}
