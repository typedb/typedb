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
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.atom.binary.Resource;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.query.AtomicQuery;
import ai.grakn.graql.internal.reasoner.query.Query;
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

    private final Query body;
    private final AtomicQuery head;

    public InferenceRule(Rule rule, GraknGraph graph){
        body = new Query(match(rule.getLHS()), graph);
        head = new AtomicQuery(match(rule.getRHS()), graph);
    }

    /**
     * @return body of the rule of the form head :- body
     */
    public Query getBody(){return body;}

    /**
     * @return head of the rule of the form head :- body
     */
    public AtomicQuery getHead(){return head;}

    /**
     * @return a conclusion atom which parent contains all atoms in the rule
     */
    public Atom getRuleConclusionAtom() {
        Query ruleQuery = new Query(head);
        Atom atom = ruleQuery.selectAtoms().iterator().next();
        body.getAtoms().forEach(at -> ruleQuery.addAtom(at.clone()));
        return atom;
    }

    private void propagateConstraints(Atom parentAtom){
        if(parentAtom.isRelation() || parentAtom.isResource()) {
            Set<Atom> types = parentAtom.getTypeConstraints().stream()
                    .filter(type -> !body.containsEquivalentAtom(type))
                    .collect(Collectors.toSet());
            Set<Predicate> predicates = parentAtom.getPredicates();
            if(parentAtom.isResource())
                    predicates.addAll(((Resource) parentAtom).getMultiPredicate());

            head.addAtomConstraints(predicates);
            body.addAtomConstraints(predicates);
            head.addAtomConstraints(types);
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
