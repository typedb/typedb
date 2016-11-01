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
package ai.grakn.graql.internal.reasoner.atom;

import ai.grakn.GraknGraph;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.reasoner.query.Query;
import ai.grakn.util.ErrorMessage;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javafx.util.Pair;

public abstract class Atom extends AtomBase {

    protected Type type = null;
    protected String typeId = null;

    public Atom(VarAdmin pattern) {
        super(pattern);
        this.atomPattern = pattern;
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    public Atom(VarAdmin pattern, Query par) {
        super(pattern, par);
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    public Atom(Atom a) {
        super(a);
        this.typeId = extractTypeId(atomPattern.asVar());
    }

    private String extractTypeId(VarAdmin var) {
        String vTypeId;
        Map<VarAdmin, Set<ValuePredicateAdmin>> resourceMap = var.getResourcePredicates();
        if (resourceMap.size() != 0) {
            if (resourceMap.size() != 1)
                throw new IllegalArgumentException(ErrorMessage.MULTIPLE_RESOURCES.getMessage(var.toString()));
            Map.Entry<VarAdmin, Set<ValuePredicateAdmin>> entry = resourceMap.entrySet().iterator().next();
            vTypeId = entry.getKey().getId().orElse("");
        }
        else
            vTypeId = var.getType().flatMap(VarAdmin::getId).orElse("");
        return vTypeId;
    }

    @Override
    public boolean isAtom(){ return true;}

    /**
     * @return true if the atom corresponds to a atom
     * */
    public boolean isType(){ return false;}

    /**
     * @return true if the atom corresponds to a non-unary atom
     * */
    public boolean isRelation(){return false;}

    /**
     * @return true if the atom corresponds to a resource atom
     * */
    public boolean isResource(){ return false;}

    @Override
    public boolean isRuleResolvable() {
        Type type = getType();
        return type != null && !getType().getRulesOfConclusion().isEmpty();
    }

    @Override
    public boolean isRecursive(){
        if (isResource()) return false;
        boolean atomRecursive = false;

        String typeId = getTypeId();
        if (typeId.isEmpty()) return false;
        Type type = getParentQuery().getGraph().orElse(null).getType(typeId);
        Collection<Rule> presentInConclusion = type.getRulesOfConclusion();
        Collection<Rule> presentInHypothesis = type.getRulesOfHypothesis();

        for(Rule rule : presentInConclusion)
            atomRecursive |= presentInHypothesis.contains(rule);

        return atomRecursive;
    }

    public MatchQuery getMatchQuery(GraknGraph graph) {
        QueryBuilder qb = Graql.withGraph(graph);
        MatchQuery matchQuery = qb.match(getPattern());

        //add IdPredicates
        Map<String, Predicate> varSubMap = getVarSubMap();
        Set<String> selectVars = getVarNames();
        varSubMap.forEach( (var, sub) -> {
            Set<PatternAdmin> patterns = new HashSet<>();
            patterns.add(sub.getPattern());
            matchQuery.admin().getPattern().getPatterns().add(Patterns.conjunction(patterns));
        });
        return matchQuery.admin().select(selectVars);
    }

    public Type getType(){
        if (type == null)
            type = getParentQuery().getGraph().orElse(null).getType(typeId);
        return type;
    }

    public String getTypeId(){ return typeId;}

    public String getValueVariable() {
        throw new IllegalArgumentException("getValueVariable called on Atom object " + getPattern());
    }

    @Override
    public Map<String, String> getUnifiers(Atomic parentAtom) {
        if (!(parentAtom instanceof Atom))
            throw new IllegalArgumentException(ErrorMessage.UNIFICATION_ATOM_INCOMPATIBILITY.getMessage());

        Set<String> varsToAllocate = parentAtom.getVarNames();
        Set<String> childBVs = getVarNames();
        Map<String, String> unifiers = new HashMap<>();
        Map<String, Pair<Type, RoleType>> childMap = getVarTypeRoleMap();
        Map<RoleType, Pair<String, Type>> parentMap = ((Atom) parentAtom).getRoleVarTypeMap();

        //try based on IdPredicates
        Query parentQuery = parentAtom.getParentQuery();
        getParentQuery().getIdPredicates().stream().filter(sub -> containsVar(sub.getVarName())).forEach(sub -> {
            String chVar = sub.getVarName();
            String id = sub.getPredicateValue();
            Set<Atomic> parentSubs = parentQuery.getIdPredicates().stream()
                    .filter(s -> s.getPredicateValue().equals(id)).collect(Collectors.toSet());
            String pVar = parentSubs.isEmpty()? "" : parentSubs.iterator().next().getVarName();
            if (!pVar.isEmpty()) {
                if (!chVar.equals(pVar)) unifiers.put(chVar, pVar);
                childBVs.remove(chVar);
                varsToAllocate.remove(pVar);
            }
        });

        //try based on roles
        for (String chVar : childBVs) {
            RoleType role = childMap.containsKey(chVar) ? childMap.get(chVar).getValue() : null;
            String pVar = role != null && parentMap.containsKey(role) ? parentMap.get(role).getKey() : "";
            if (pVar.isEmpty())
                pVar = varsToAllocate.iterator().next();

            if (!chVar.equals(pVar)) unifiers.put(chVar, pVar);
            varsToAllocate.remove(pVar);
        }
        return unifiers;
    }

    public Set<Predicate> getIdPredicates() {
        return getParentQuery().getIdPredicates().stream()
                .filter(atom -> containsVar(atom.getVarName()))
                .collect(Collectors.toSet());
    }

    public Set<Predicate> getValuePredicates(){
        return getParentQuery().getValuePredicates().stream()
                .filter(atom -> containsVar(atom.getVarName()))
                .collect(Collectors.toSet());
    }

    public Set<Atom> getTypeConstraints(){
        return getParentQuery().getTypeConstraints().stream()
                .filter(atom -> containsVar(atom.getVarName()))
                .collect(Collectors.toSet());
    }

    public Map<String, Predicate> getVarSubMap() {
        Map<String, Predicate> map = new HashMap<>();
        getIdPredicates().forEach( sub -> {
            String var = sub.getVarName();
            map.put(var, sub);
        });
        return map;
    }

    public Map<RoleType, String> getRoleConceptIdMap(){
        Map<RoleType, String> roleConceptMap = new HashMap<>();
        Map<String, Predicate> varSubMap = getVarSubMap();
        Map<RoleType, Pair<String, Type>> roleVarMap = getRoleVarTypeMap();

        roleVarMap.forEach( (role, varTypePair) -> {
            String var = varTypePair.getKey();
            roleConceptMap.put(role, varSubMap.containsKey(var) ? varSubMap.get(var).getPredicateValue() : "");
        });
        return roleConceptMap;
    }

    public Map<String, javafx.util.Pair<Type, RoleType>> getVarTypeRoleMap() {
        Map<String, javafx.util.Pair<Type, RoleType>> roleVarTypeMap = new HashMap<>();
        if (getParentQuery() == null) return roleVarTypeMap;

        Set<String> vars = getVarNames();
        Map<String, Type> varTypeMap = getParentQuery().getVarTypeMap();

        vars.forEach(var -> {
            Type type = varTypeMap.get(var);
            roleVarTypeMap.put(var, new Pair<>(type, null));
        });
        return roleVarTypeMap;
    }

    public Map<RoleType, Pair<String, Type>> getRoleVarTypeMap() { return new HashMap<>();}
}
