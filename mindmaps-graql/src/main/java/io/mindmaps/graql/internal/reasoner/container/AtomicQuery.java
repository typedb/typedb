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

package io.mindmaps.graql.internal.reasoner.container;

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

import static io.mindmaps.graql.internal.reasoner.Utility.computeRoleCombinations;

public class AtomicQuery extends Query{

    final private Atomic atom;
    final private Set<AtomicQuery> children = new HashSet<>();

    public AtomicQuery(String rhs, MindmapsGraph graph){
        super(rhs, graph);
        if(atomSet.size() > 1)
            throw new IllegalArgumentException(ErrorMessage.NON_ATOMIC_QUERY.getMessage());
        atom = atomSet.iterator().next();
    }

    public AtomicQuery(AtomicQuery q){
        super(q);
        atom = atomSet.iterator().next();
    }

    public AtomicQuery(Atomic at) {
        super(at);
        atom = at;
    }

    public void addChild(AtomicQuery q){ children.add(q);}
    public Atomic getAtom(){ return atom;}
    public Set<AtomicQuery> getChildren(){ return children;}

    private void materialize() {
        if (!getMatchQuery().ask().execute()) {
            InsertQuery insert = Graql.insert(getPattern().getVars()).withGraph(graph);
            insert.execute();
        }
    }

    public void materialize(Set<Substitution> subs) {
        subs.forEach(this::addAtom);

        //extrapolate if needed
        Atomic atom = selectAtoms().iterator().next();
        if(atom.isRelation() && (atom.getRoleVarTypeMap().isEmpty() || !((Relation) atom).hasExplicitRoleTypes() )){
            String relTypeId = atom.getTypeId();
            RelationType relType = graph.getRelationType(relTypeId);
            Set<String> vars = atom.getVarNames();
            Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());

            Set<Map<String, String>> roleMaps = new HashSet<>();
            computeRoleCombinations(vars, roles, new HashMap<>(), roleMaps);

            removeAtom(atom);
            roleMaps.forEach( map -> {
                Relation relationWithRoles = new Relation(relTypeId, map);
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

}
