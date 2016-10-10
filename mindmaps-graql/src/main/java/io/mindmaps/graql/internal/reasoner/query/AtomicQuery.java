/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.reasoner.query;

import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.graql.internal.reasoner.predicate.Relation;
import io.mindmaps.graql.internal.reasoner.predicate.Substitution;
import io.mindmaps.util.ErrorMessage;

import java.util.*;
import java.util.stream.Collectors;

import static io.mindmaps.graql.internal.reasoner.Utility.computeRoleCombinations;

public class AtomicQuery extends Query{

    private Atomic atom;
    private AtomicQuery parent = null;

    final private Set<AtomicQuery> children = new HashSet<>();

    public AtomicQuery(String rhs, MindmapsGraph graph){
        super(rhs, graph);
        if(selectAtoms().size() > 1)
            throw new IllegalArgumentException(ErrorMessage.NON_ATOMIC_QUERY.getMessage());
        atom = selectAtoms().iterator().next();
    }

    public AtomicQuery(AtomicQuery q){
        super(q);
        Atomic coreAtom = null;
        Iterator<Atomic> it = atomSet.iterator();
        while(it.hasNext() && coreAtom == null) {
            Atomic at = it.next();
            if (at.equals(q.getAtom())) coreAtom = at;
        }
        atom = coreAtom;

        parent = q.getParent();
        children.addAll(q.getChildren());
    }

    public AtomicQuery(Atomic at) {
        super(at);
        atom = at;
    }

    @Override
    public boolean equals(Object obj){
        if (!(obj instanceof AtomicQuery)) return false;
        AtomicQuery a2 = (AtomicQuery) obj;
        return this.isEquivalent(a2);
    }

    public void addChild(AtomicQuery q){
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
        Atomic aqAtom = aq.getAtom();
        if(atom.getTypeId().equals(aqAtom.getTypeId())) {
            if (atom.isRelation() && aqAtom.getRoleVarTypeMap().size() > atom.getRoleVarTypeMap().size())
                aq.addChild(this);
            else
                this.addChild(aq);
        }
    }

    public Atomic getAtom(){ return atom;}
    public Set<AtomicQuery> getChildren(){ return children;}

    private void materialize() {
        if( getAtoms().stream().filter(Atomic::isSubstitution).collect(Collectors.toSet()).size() != getVarSet().size())
            throw new IllegalStateException(ErrorMessage.MATERIALIZATION_ERROR.getMessage(this.toString()));
        if (!getMatchQuery().ask().execute()) {
            InsertQuery insert = Graql.insert(getPattern().getVars()).withGraph(graph);
            insert.execute();
        }
    }

    public void materialize(Set<Substitution> subs) {
        subs.forEach(this::addAtom);

        //extrapolate if needed
        Atomic atom = getAtom();
        if(atom.isRelation() && (atom.getRoleVarTypeMap().isEmpty() || !((Relation) atom).hasExplicitRoleTypes() )){
            String relTypeId = atom.getTypeId();
            RelationType relType = graph.getRelationType(relTypeId);
            Set<String> vars = atom.getVarNames();
            Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());

            Set<Map<String, String>> roleMaps = new HashSet<>();
            computeRoleCombinations(vars, roles, new HashMap<>(), roleMaps);

            removeAtom(atom);
            roleMaps.forEach( map -> {
                Relation relationWithRoles = new Relation(relTypeId, map, this.parent);
                addAtom(relationWithRoles);
                materialize();
                removeAtom(relationWithRoles);
            });
            addAtom(atom);
        }
        else
            materialize();

        subs.forEach(this::removeAtom);
    }

    @Override
    public void unify(Map<String, String> unifiers) {
        super.unify(unifiers);
        atom = selectAtoms().iterator().next();
    }
}
