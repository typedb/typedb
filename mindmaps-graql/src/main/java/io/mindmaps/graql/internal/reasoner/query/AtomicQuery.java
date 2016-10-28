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
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.internal.reasoner.atom.Atom;
import io.mindmaps.graql.internal.reasoner.atom.Atomic;
import io.mindmaps.graql.internal.reasoner.atom.Relation;
import io.mindmaps.graql.internal.reasoner.atom.Substitution;
import io.mindmaps.util.ErrorMessage;

import java.util.*;
import java.util.stream.Collectors;

import static io.mindmaps.graql.internal.reasoner.Utility.computeRoleCombinations;

public class AtomicQuery extends Query{

    private Atom atom;
    private AtomicQuery parent = null;

    final private Set<AtomicQuery> children = new HashSet<>();

    public AtomicQuery(String rhs, MindmapsGraph graph){
        super(rhs, graph);
        atom = selectAtoms().iterator().next();
    }

    public AtomicQuery(MatchQuery query, MindmapsGraph graph){
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

    public AtomicQuery(Atom at) {
        super(at);
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
        /*
        if( getAtoms().stream()
                .filter(Atomic::isPredicate)
                .collect(Collectors.toSet()).size() < getVarSet().size()) {
            System.out.println("Materialising: ");
            getPattern().getVars().forEach(System.out::println);
            throw new IllegalStateException(ErrorMessage.MATERIALIZATION_ERROR.getMessage(getMatchQuery().toString()));
        }
        */
        if (!getMatchQuery().ask().execute()) {
            InsertQuery insert = Graql.insert(getPattern().getVars()).withGraph(graph);
            //System.out.println("Materialising: ");
            //getPattern().getVars().forEach(System.out::println);
            insert.stream()
                    .filter(Concept::isResource)
                    .forEach(c -> {
                        Map<String, Concept> answer = new HashMap<>();
                        answer.put(atom.getVarName(), graph.getEntity(getSubstitution(atom.getVarName())));
                        answer.put(atom.getValueVariable(), c);
                        insertAnswers.add(answer);
                    });
            //String test = "match $x isa applicant;";
            //System.out.println("No of applicants: " + Sets.newHashSet(Graql.withGraph(graph).<MatchQuery>parse(test)).size());
        }
        else{
            //System.out.println("Not materialising, concepts already exist");
        }
        return insertAnswers;
    }

    public QueryAnswers materialise(){ return materialiseComplete();}

    /**
     * Add explicit substitutions and materialise
     * @subs substitutions of variables
     */
    public QueryAnswers materialise(Set<Substitution> subs) {
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
            computeRoleCombinations(vars, roles, new HashMap<>(), roleMaps);

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
