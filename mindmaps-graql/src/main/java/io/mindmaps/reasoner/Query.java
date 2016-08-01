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

package io.mindmaps.reasoner;

import com.google.common.collect.Sets;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Type;
import io.mindmaps.graql.api.parser.QueryParser;
import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.MatchQuery;
import io.mindmaps.graql.api.query.Var;
import org.javatuples.*;

import java.util.*;


class Query {

    //TODO remove graph dependency
    private final MindmapsTransaction graph;

    private final Set<Atom> atomSet;
    private final Map<Type, Set<Atom>> typeAtomMap;
    private final Map<Atom, Pattern.Admin> atomDisjunctionMap = new HashMap<>();

    private final MatchQuery selectQuery;

    public Query(String query, MindmapsTransaction transaction) {
        this.graph = transaction;
        QueryParser qp = QueryParser.create(graph);
        this.selectQuery = qp.parseMatchQuery(query).getMatchQuery();
        this.atomSet = getAtomSet(selectQuery);
        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    public Query(MatchQuery query, MindmapsTransaction transaction) {
        this.graph = transaction;
        this.selectQuery = query;
        this.atomSet = getAtomSet(selectQuery);
        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    public Query(Query q) {
        this.graph = q.graph;
        QueryParser qp = QueryParser.create(graph);
        selectQuery = qp.parseMatchQuery(q.toString()).getMatchQuery();
        this.atomSet = getAtomSet(selectQuery);

        for (Atom qAtom : q.atomSet) {
            Set<Query> expansions = qAtom.getExpansions();
            for (Query exp : expansions) {
                atomSet.forEach(atom ->
                {
                    if (atom.equals(qAtom)) atom.addExpansion(new Query(exp));
                });
            }
        }

        this.typeAtomMap = getTypeAtomMap(atomSet);
    }

    @Override
    public String toString() {
        return selectQuery.toString();
    }

    public void printAtoms() {
        atomSet.forEach(Atom::print);
    }

    public void printTypeAtomMap() {
        for (Map.Entry<Type, Set<Atom>> entry : typeAtomMap.entrySet()) {
            System.out.println("type: " + entry.getKey());
            entry.getValue().forEach(a -> System.out.println("atom: " + a.toString()));
        }
    }

    public Set<Atom> getAtoms() {
        return atomSet;
    }

    public Set<Atom> getAtomsWithType(Type type) {
        return typeAtomMap.get(type);
    }

    public Set<String> getVarSet() {
        Set<String> vars = new HashSet<>();
        atomSet.forEach(atom -> vars.addAll(atom.getVarNames()));
        return vars;
    }

    public void expandAtomByQuery(Atom atom, Query query) {
        atomSet.stream().filter(a -> a.equals(atom)).forEach(a -> atom.addExpansion(query));
    }

    public boolean containsVar(String var) {
        boolean varContained = false;
        for (Atom atom : atomSet)
            varContained |= atom.containsVar(var);

        return varContained;
    }

    private void replacePattern(Pattern.Admin oldPattern, Pattern.Admin newPattern) {
        selectQuery.admin().getPattern().getPatterns().remove(oldPattern);
        selectQuery.admin().getPattern().getPatterns().add(newPattern);
    }

    private void changeAtomVarName(Atom oldAtom, String from, String to) {
        if (oldAtom.isRelation())
            oldAtom.changeRelVarName(from, to);
        else
            oldAtom.setVarName(to);
    }


    public void changeVarName(String from, String to) {
        String replacement = containsVar(to) ? to + to : to;
        atomSet.stream().filter(atom -> atom.containsVar(from)).forEach(atom -> changeAtomVarName(atom, from, replacement));
    }

    public MatchQuery getMatchQuery() {
        return selectQuery;
    }

    public MatchQuery getExpandedMatchQuery() {
        for (Atom atom : atomSet) {
            Set<Query> expansions = atom.getExpansions();
            if (!expansions.isEmpty()) {

                Pattern.Admin atomPtrn = atom.getPattern();
                Set<Pattern.Admin> expandedPattern = new HashSet<>();
                expandedPattern.add(atomPtrn);
                expansions.forEach(q -> expandedPattern.add(q.getExpandedPattern()));

                //atom sits in disjunction
                if (atomDisjunctionMap.containsKey(atom)) {
                    Pattern.Admin disjunction = atomDisjunctionMap.get(atom);
                    Set<Pattern.Admin> disjunctionPatterns = new HashSet<>();
                    disjunction.admin().getVars().forEach(p -> disjunctionPatterns.add(p.asVar()));
                    disjunctionPatterns.remove(atomPtrn.asVar());

                    Pattern.Admin newDisjunction = Pattern.Admin.disjunction(disjunctionPatterns);
                    expandedPattern.add(newDisjunction);

                    Pattern.Admin expandedDisjunction = Pattern.Admin.disjunction(Sets.newHashSet(expandedPattern));

                    replacePattern(disjunction, expandedDisjunction);
                } else
                    replacePattern(atomPtrn, Pattern.Admin.disjunction(expandedPattern));
            }
        }

        return selectQuery;
    }

    public Pattern.Admin getPattern() {
        return getMatchQuery().admin().getPattern();
    }

    public Pattern.Admin getExpandedPattern() {
        return getExpandedMatchQuery().admin().getPattern();
    }

    //TODO look at disjunctive normal form to sort out atoms in disjunctions
    private Set<Atom> getAtomSet(MatchQuery query) {
        Set<Atom> atoms = new HashSet<>();

        Set<Pattern.Admin> patterns = query.admin().getPattern().getPatterns();
        for (Pattern.Admin pattern : patterns) {
            if (pattern.isDisjunction()) {
                Set<Var.Admin> vars = pattern.admin().getVars();
                vars.forEach(v -> {
                    Atom atom = new Atom(v.asVar());
                    atoms.add(atom);
                    atomDisjunctionMap.put(atom, pattern);
                });
            } else {
                Atom atom = new Atom(pattern);
                atoms.add(atom);
            }
        }
        return atoms;
    }

    private Map<Type, Set<Atom>> getTypeAtomMap(Set<Atom> atoms) {
        Map<Type, Set<Atom>> map = new HashMap<>();
        for (Atom atom : atoms) {
            Type type = graph.getType(atom.getTypeId());
            if (map.containsKey(type))
                map.get(type).add(atom);
            else
                map.put(type, Sets.newHashSet(atom));
        }
        return map;

    }

    public Map<String, Type> getVarTypeMap() {
        Map<String, Type> map = new HashMap<>();
        atomSet.stream().filter(Atom::isType).forEach(atom -> map.putIfAbsent(atom.getVarName(), graph.getType(atom.getTypeId())));
        return map;
    }

    public String getValue(String var)
    {
        String val ="";
        for(Atom atom : atomSet)
        {
            if(atom.getVarName().equals(var))
                if(!atom.getVal().isEmpty() ) val = atom.getVal();
        }
        return val;
    }

    //TODO should be moved to utility
    private Set<RoleType> getCompatibleRoleTypes(String typeId, String relId)
    {
        Set<RoleType> cRoles = new HashSet<>();

        Collection<RoleType> typeRoles = graph.getType(typeId).playsRoles();
        Collection<RoleType> relRoles = graph.getRelationType(relId).hasRoles();
        relRoles.stream().filter(typeRoles::contains).forEach(cRoles::add);
        return cRoles;
    }


    /**
     * Attempts to infer the implicit roleTypes of vars in a relAtom
     * @param relAtom relation atom whose roleTypes are to be inferred
     * @return map containing a varName - varType, varRoleType triple
     */
    public Map<String, Pair<Type, RoleType>> getVarTypeRoleMap(Atom relAtom)
    {
        if (!relAtom.isRelation())
            return null;

        Map<String, Pair<Type, RoleType>> roleVarTypeMap = new HashMap<>();

        String relTypeId = relAtom.getTypeId();
        Set<String> vars = relAtom.getVarNames();
        Map<String, Type> varTypeMap = getVarTypeMap();

        for (String var : vars)
        {
            Type type = varTypeMap.get(var);
            if (type != null)
            {
                Set<RoleType> cRoles = getCompatibleRoleTypes(type.getId(), relTypeId);

                /**if roleType is unambigous*/
                if(cRoles.size() == 1)
                    roleVarTypeMap.put(var, new Pair<>(type, cRoles.iterator().next()));
                else
                    roleVarTypeMap.put(var, new Pair<>(type, null));

            }
        }
        return roleVarTypeMap;
    }

    /**
     * Attempts to infer the implicit roleTypes and matching types
     * @param relAtom relation atom whose roleTypes are to be inferred
     * @return map containing a RoleType-Type pair
     */
    public Map<RoleType, Pair<String, Type>> getRoleVarTypeMap(Atom relAtom)
    {
        if (!relAtom.isRelation() )
            return null;

        Map<RoleType, Pair<String, Type>> roleVarTypeMap = new HashMap<>();

        String relTypeId = relAtom.getTypeId();
        Set<String> vars = relAtom.getVarNames();
        Map<String, Type> varTypeMap = getVarTypeMap();

        for (String var : vars)
        {

            Type type = varTypeMap.get(var);
            if (type != null)
            {
                Set<RoleType> cRoles = getCompatibleRoleTypes(type.getId(), relTypeId);

                /**if roleType is unambigous*/
                if(cRoles.size() == 1)
                    roleVarTypeMap.put(cRoles.iterator().next(), new Pair<>(var, type));

            }
        }
        return roleVarTypeMap;
    }
}
