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
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.property.IsaProperty;
import ai.grakn.graql.internal.pattern.property.RelationProperty;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.atom.binary.Binary;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.graql.internal.reasoner.query.AtomicQuery;
import ai.grakn.graql.internal.reasoner.query.Query;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class InferenceRule {

    private final Query body;
    private final AtomicQuery head;

    public InferenceRule(Rule rule, GraknGraph graph){
        QueryBuilder qb = graph.graql();
        body = new Query(qb.match(rule.getLHS()), graph);
        head = new AtomicQuery(qb.match(rule.getRHS()), graph);
    }

    public Query getBody(){return body;}
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
            Set<Predicate> predicates = new HashSet<>();
            //predicates obtained from types
            types.stream().map(type -> (Binary) type)
                    .filter(type -> type.getPredicate() != null)
                    .map(Binary::getPredicate)
                    .forEach(predicates::add);
            //direct predicates
            predicates.addAll(parentAtom.getPredicates());

            head.addAtomConstraints(predicates);
            body.addAtomConstraints(predicates);
            head.addAtomConstraints(types);
            body.addAtomConstraints(types);
        }
    }

    private void rewriteHead(Atom parentAtom){
        if(parentAtom.isUserDefinedName() && parentAtom.isRelation() ){
            Relation childAtom = (Relation) head.getAtom();
            VarAdmin var = childAtom.getPattern().asVar();
            Var relVar = Graql.var(parentAtom.getVarName());
            var.getProperty(IsaProperty.class).ifPresent(prop -> relVar.isa(prop.getType()));
            // This is guaranteed to be a relation
            //noinspection OptionalGetWithoutIsPresent
            var.getProperty(RelationProperty.class).get().getRelationPlayers()
                    .forEach(c -> {
                VarAdmin rolePlayer = c.getRolePlayer();
                Optional<VarAdmin> roleType = c.getRoleType();
                if (roleType.isPresent())
                    relVar.rel(roleType.get(), rolePlayer);
                else
                    relVar.rel(rolePlayer);
            });

            Relation newAtom = new Relation(relVar.admin(), childAtom.getPredicate(), head);
            head.removeAtom(childAtom);
            head.addAtom(newAtom);
        }
    }

    /**
     * propagate variables to child via a relation atom (atom variables are bound)
     * @param parentAtom   parent atom (atom) being resolved (subgoal)
     */
    private void unifyViaAtom(Atom parentAtom) {
        rewriteHead(parentAtom);
        
        Atomic childAtom = getRuleConclusionAtom();
        Query parent = parentAtom.getParentQuery();
        Map<String, String> unifiers = childAtom.getUnifiers(parentAtom);

        //do alpha-conversion
        head.unify(unifiers);
        body.unify(unifiers);

        //check free variables for possible captures
        Set<String> childFVs = body.getVarSet();
        Set<String> parentBVs = parentAtom.getVarNames();
        Set<String> parentVars = parent.getVarSet();
        parentBVs.forEach(childFVs::remove);

        childFVs.forEach(chVar -> {
            // if (x e P) v (x e G)
            // x -> fresh
            if (parentVars.contains(chVar)) {
                String freshVar = Utility.createFreshVariable(body.getVarSet(), chVar);
                body.unify(chVar, freshVar);
            }
        });
    }

    /**
     * make child query consistent by performing variable IdPredicate so that parent variables are propagated
     * @param parentAtom   parent atom (atom) being resolved (subgoal)
     */
   public void unify(Atom parentAtom) {
        unifyViaAtom(parentAtom);
        propagateConstraints(parentAtom);
    }
}
