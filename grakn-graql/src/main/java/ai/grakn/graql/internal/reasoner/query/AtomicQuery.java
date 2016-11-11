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

package ai.grakn.graql.internal.reasoner.query;

import ai.grakn.GraknGraph;
import ai.grakn.concept.RelationType;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.atom.Relation;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import ai.grakn.concept.Concept;
import ai.grakn.concept.RoleType;
import ai.grakn.graql.Graql;
import ai.grakn.graql.internal.reasoner.atom.IdPredicate;

import java.util.*;
import java.util.stream.Collectors;

public class AtomicQuery extends Query{

    private Atom atom;
    private AtomicQuery parent = null;

    final private Set<AtomicQuery> children = new HashSet<>();

    public AtomicQuery(String rhs, GraknGraph graph){
        super(rhs, graph);
        atom = selectAtoms().iterator().next();
    }

    public AtomicQuery(MatchQuery query, GraknGraph graph){
        super(query, graph);
        atom = selectAtoms().iterator().next();
    }

    public AtomicQuery(AtomicQuery q){
        super(q);
        Atom coreAtom = null;
        Iterator<Atom> it = atomSet.stream().filter(Atomic::isAtom).map(at -> (Atom) at).iterator();
        while(it.hasNext() && coreAtom == null) {
            Atom at = it.next();
            if (at.equals(q.getAtom())) coreAtom = at;
        }
        atom = coreAtom;
        parent = q.getParent();
        children.addAll(q.getChildren());
    }

    public AtomicQuery(Atom at, Set<String> vars) {
        super(at, vars);
        atom = selectAtoms().iterator().next();
    }

    @Override
    public boolean equals(Object obj){
        if (!(obj instanceof AtomicQuery)) return false;
        AtomicQuery a2 = (AtomicQuery) obj;
        return this.isEquivalent(a2);
    }

    private void addChild(AtomicQuery q){
        if (!this.isEquivalent(q) && atom.getTypeId().equals(q.getAtom().getTypeId())){
            children.add(q);
            q.setParent(this);
        }
    }
    private void setParent(AtomicQuery q){ parent = q;}
    public AtomicQuery getParent(){ return parent;}

    /**
     * establishes parent-child (if there is one) relation between this and aq query
     * the relation expresses the relative level of specificity between queries with the parent being more specific
     * @param aq query to compare
     */
    public void establishRelation(AtomicQuery aq){
        Atom aqAtom = aq.getAtom();
        if(atom.getTypeId().equals(aqAtom.getTypeId())) {
            if (atom.isRelation() && aqAtom.getRoleVarTypeMap().size() > atom.getRoleVarTypeMap().size())
                aq.addChild(this);
            else
                this.addChild(aq);
        }
    }

    public Atom getAtom(){ return atom;}
    public Set<AtomicQuery> getChildren(){ return children;}

    /**
     * materialise the query provided all variables are mapped
     */
    private QueryAnswers materialiseComplete() {
        QueryAnswers insertAnswers = new QueryAnswers();
        if( getAtoms().stream()
                .filter(Atomic::isPredicate)
                .collect(Collectors.toSet()).size() < getVarSet().size()) {
            throw new IllegalStateException(ErrorMessage.MATERIALIZATION_ERROR.getMessage());
        }
        if (!getMatchQuery().ask().execute()) {
            InsertQuery insert = Graql.insert(getPattern().getVars()).withGraph(graph);
            insert.stream()
                    .filter(Concept::isResource)
                    .forEach(c -> {
                        Map<String, Concept> answer = new HashMap<>();
                        answer.put(atom.getVarName(), graph.getEntity(getIdPredicate(atom.getVarName()).getPredicateValue()));
                        answer.put(atom.getValueVariable(), c);
                        insertAnswers.add(answer);
                    });
        }
        return insertAnswers;
    }

    public QueryAnswers materialise(){ return materialiseComplete();}

    /**
     * Add explicit IdPredicates and materialise
     * @subs IdPredicates of variables
     */
    public QueryAnswers materialise(Set<IdPredicate> subs) {
        QueryAnswers insertAnswers = new QueryAnswers();
        subs.forEach(this::addAtom);

        //extrapolate if needed
        Atom atom = getAtom();
        if(atom.isRelation() &&
                (atom.getRoleVarTypeMap().isEmpty() || !((Relation) atom).hasExplicitRoleTypes() )){
            String relTypeId = atom.getTypeId();
            RelationType relType = (RelationType) atom.getType();
            Set<String> vars = atom.getVarNames();
            Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());

            Set<Map<String, String>> roleMaps = new HashSet<>();
            Utility.computeRoleCombinations(vars, roles, new HashMap<>(), roleMaps);

            removeAtom(atom);
            roleMaps.forEach( map -> {
                Relation relationWithRoles = new Relation(relTypeId, map, this.parent);
                addAtom(relationWithRoles);
                insertAnswers.addAll(materialiseComplete());
                removeAtom(relationWithRoles);
            });
            addAtom(atom);
        }
        else
            insertAnswers.addAll(materialiseComplete());

        subs.forEach(this::removeAtom);
        return insertAnswers;
    }

    @Override
    public void unify(Map<String, String> unifiers) {
        super.unify(unifiers);
        atom = selectAtoms().iterator().next();
    }

    @Override
    public Set<Atom> selectAtoms() {
        Set<Atom> selectedAtoms = super.selectAtoms();
        if (selectedAtoms.size() != 1)
            throw new IllegalStateException(ErrorMessage.NON_ATOMIC_QUERY.getMessage(this.toString()));
        return selectedAtoms;
    }
}
