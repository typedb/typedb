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
import ai.grakn.concept.Concept;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.internal.reasoner.Utility;
import ai.grakn.graql.internal.reasoner.atom.Atom;
import ai.grakn.graql.internal.reasoner.atom.Atomic;
import ai.grakn.graql.internal.reasoner.atom.binary.Relation;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.Sets;
import ai.grakn.concept.Concept;
import ai.grakn.concept.RoleType;
import ai.grakn.graql.Graql;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javafx.util.Pair;

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
        atom = selectAtoms().stream().findFirst().orElse(null);
        parent = q.getParent();
        children.addAll(q.getChildren());
    }

    public AtomicQuery(Atom at, Set<String> vars) {
        super(at, vars);
        atom = selectAtoms().stream().findFirst().orElse(null);
    }

    @Override
    public boolean equals(Object obj){
        if (obj == null || this.getClass() != obj.getClass()) return false;
        if (obj == this) return true;
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

    @Override
    public boolean addAtom(Atomic at) {
        if(super.addAtom(at)){
            if(atom == null && at.isSelectable()) atom = (Atom) at;
            return true;
        }
        else return false;
    }

    @Override
    public boolean removeAtom(Atomic at) {
        if( super.removeAtom(at)) {
            if (atom != null & at.equals(atom)) atom = null;
            return true;
        }
        else return false;
    }

    /**
     * materialise the query provided all variables are mapped
     */
    private QueryAnswers materialiseComplete() {
        Atom atom = selectAtoms().iterator().next();
        QueryAnswers insertAnswers = new QueryAnswers();
        if (!getMatchQuery().ask().execute()) {
            InsertQuery insert = Graql.insert(getPattern().getVars()).withGraph(graph);
            Set<Concept> insertedConcepts = insert.stream().collect(Collectors.toSet());
            if (atom.isUserDefinedName()) {
                insertedConcepts.stream()
                        .filter(c -> c.isResource() || c.isRelation())
                        .forEach(c -> {
                            Map<String, Concept> answer = new HashMap<>();
                            if (c.isResource()) {
                                answer.put(atom.getVarName(), graph.getConcept(getIdPredicate(atom.getVarName()).getPredicateValue()));
                                answer.put(atom.getValueVariable(), c);
                            } else if (c.isRelation()) {
                                answer.put(atom.getVarName(), c);
                                Map<RoleType, Pair<String, Type>> roleMap = atom.getRoleVarTypeMap();
                                Map<RoleType, Instance> roleplayers = ((ai.grakn.concept.Relation) c).rolePlayers()
                                        .entrySet().stream()
                                        .filter(entry -> entry.getValue() != null)
                                        .filter(entry -> roleMap.containsKey(entry.getKey()))
                                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                                roleplayers.entrySet().forEach(entry ->
                                        answer.put(roleMap.get(entry.getKey()).getKey(), entry.getValue()));
                            }
                            insertAnswers.add(answer);
                        });
            }
        }
        return insertAnswers;
    }

    public QueryAnswers materialise(){ return materialiseComplete();}

    /**
     * Add explicit IdPredicates and materialise
     * @param subs IdPredicates of variables
     * @return Materialised answers
     */
    public QueryAnswers materialise(Set<IdPredicate> subs) {
        QueryAnswers insertAnswers = new QueryAnswers();
        AtomicQuery queryToMaterialise = new AtomicQuery(this);
        subs.forEach(queryToMaterialise::addAtom);

        //extrapolate if needed
        Atom at = queryToMaterialise.getAtom();
        if(at.isRelation()){
            Relation relAtom = (Relation) at;
            Set<String> rolePlayers = relAtom.getRolePlayers();
            if (relAtom.getRoleVarTypeMap().size() != rolePlayers.size()) {
                RelationType relType = (RelationType) atom.getType();
                Set<RoleType> roles = Sets.newHashSet(relType.hasRoles());
                Set<Map<String, String>> roleMaps = new HashSet<>();
                Utility.computeRoleCombinations(rolePlayers , roles, new HashMap<>(), roleMaps);

                queryToMaterialise.removeAtom(relAtom);
                roleMaps.forEach(roleMap -> {
                    Relation relationWithRoles = new Relation(atom.getVarName(), relAtom.getValueVariable(),
                            roleMap, relAtom.getPredicate(), queryToMaterialise);
                    queryToMaterialise.addAtom(relationWithRoles);
                    insertAnswers.addAll(queryToMaterialise.materialiseComplete());
                    queryToMaterialise.removeAtom(relationWithRoles);
                });
            }
            else
                insertAnswers.addAll(queryToMaterialise.materialiseComplete());
        }
        else
            insertAnswers.addAll(queryToMaterialise.materialiseComplete());
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
