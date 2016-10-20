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

package io.mindmaps.graql.internal.reasoner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.*;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.Var;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.pattern.Patterns;
import io.mindmaps.graql.internal.reasoner.query.AtomicQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.internal.reasoner.predicate.Atomic;
import io.mindmaps.util.ErrorMessage;
import javafx.util.Pair;

import java.util.*;

public class Utility {

    public static void printMatchQueryResults(MatchQuery sq) {
        List<Map<String, Concept>> results = Lists.newArrayList(sq);

        for (Map<String, Concept> result : results) {
            for (Map.Entry<String, Concept> entry : result.entrySet()) {
                Concept concept = entry.getValue();
                System.out.print(entry.getKey() + ": " + concept.getId() + " : ");

                if (concept.isResource()) {
                    System.out.print(concept.asResource().getValue() + " ");
                }
            }
            System.out.println();
        }
    }

    public static void printAnswers(Set<Map<String, Concept>> answers) {
        for (Map<String, Concept> result : answers) {
            for (Map.Entry<String, Concept> entry : result.entrySet()) {
                Concept concept = entry.getValue();
                System.out.print(entry.getKey() + ": " + concept.getId() + " : ");

                if (concept.isResource()) {
                    System.out.print(concept.asResource().getValue() + " ");
                }
            }
            System.out.println();
        }
    }

    public static AtomicQuery findEquivalentAtomicQuery(AtomicQuery query, Set<AtomicQuery> queries) {
        AtomicQuery equivalentQuery = null;
        Iterator<AtomicQuery> it = queries.iterator();
        while( it.hasNext() && equivalentQuery == null) {
            AtomicQuery current = it.next();
            if (query.isEquivalent(current))
                equivalentQuery = current;
        }
        return equivalentQuery;
    }

    public static Set<RoleType> getCompatibleRoleTypes(String typeId, String relId, MindmapsGraph graph) {
        Set<RoleType> cRoles = new HashSet<>();

        Collection<RoleType> typeRoles = graph.getType(typeId).playsRoles();
        Collection<RoleType> relRoles = graph.getRelationType(relId).hasRoles();
        relRoles.stream().filter(typeRoles::contains).forEach(cRoles::add);
        return cRoles;
    }

    public static boolean checkAtomsCompatible(Atomic a, Atomic b, MindmapsGraph graph) {
        if (!(a.isType() && b.isType()) || (a.isRelation() || b.isRelation())
           || !a.getVarName().equals(b.getVarName()) || a.isResource() || b.isResource()) return true;
        String aTypeId = a.getTypeId();
        Type aType = graph.getType(aTypeId);
        String bTypeId = b.getTypeId();
        Type bType = graph.getType(bTypeId);

        return checkTypesCompatible(aType, bType) && (a.getVal().isEmpty() || b.getVal().isEmpty() || a.getVal().equals(b.getVal()) );
    }

    //rolePlayer-roleType maps
    public static void computeRoleCombinations(Set<String> vars, Set<RoleType> roles, Map<String, String> roleMap,
                                        Set<Map<String, String>> roleMaps){
        Set<String> tempVars = Sets.newHashSet(vars);
        Set<RoleType> tempRoles = Sets.newHashSet(roles);
        String var = vars.iterator().next();

        roles.forEach(role -> {
            tempVars.remove(var);
            tempRoles.remove(role);
            roleMap.put(var, role.getId());
            if (!tempVars.isEmpty() && !tempRoles.isEmpty())
                computeRoleCombinations(tempVars, tempRoles, roleMap, roleMaps);
            else {
                if (!roleMap.isEmpty())
                    roleMaps.add(Maps.newHashMap(roleMap));
                roleMap.remove(var);
            }
            tempVars.add(var);
            tempRoles.add(role);
        });
    }

    public static Var createRelationVar(RelationType relType){
        Var var = Graql.var();
        Collection<RoleType> roles = relType.hasRoles();
        Set<String> vars = new HashSet<>();

        roles.forEach(role -> {
            String varName = createFreshVariable(vars, "x");
            var.rel(role.getId(), varName);
            vars.add(varName);
        });
        return var;
    }

    /**
     * generate a fresh variable avoiding global variables and variables from the same query
     * @param vars  vars to be avoided
     * @param var        variable to be generated a fresh replacement
     * @return fresh variables
     */
    public static String createFreshVariable(Set<String> vars, String var) {
        String fresh = var;
        while (vars.contains(fresh)) {
            String valFree = fresh.replaceAll("[^0-9]", "");
            int value = valFree.equals("") ? 0 : Integer.parseInt(valFree);
            fresh = fresh.replaceAll("\\d+", "") + (++value);
        }
        return fresh;
    }

    /**
     * create transitive rule R(from: X, to: Y) :- R(from: X,to: Z), R(from: Z, to: Y)
     * @param ruleId rule identifier
     * @param relType transitive relation type
     * @param fromRoleId  from directional role type id
     * @param toRoleId to directional role type id
     * @param graph graph
     * @return rule instance
     */
    public static Rule createTransitiveRule(String ruleId, RelationType relType, String fromRoleId, String toRoleId, MindmapsGraph graph){
        final int arity = relType.hasRoles().size();
        if (arity != 2)
            throw new IllegalArgumentException(ErrorMessage.RULE_CREATION_ARITY_ERROR.getMessage());

        VarAdmin startVar = Graql.var().isa(relType.getId()).rel(fromRoleId, "x").rel(toRoleId, "z").admin();
        VarAdmin endVar = Graql.var().isa(relType.getId()).rel(fromRoleId, "z").rel(toRoleId, "y").admin();
        VarAdmin headVar = Graql.var().isa(relType.getId()).rel(fromRoleId, "x").rel(toRoleId, "y").admin();
        String body = Patterns.conjunction(Sets.newHashSet(startVar, endVar)).toString() + ";";
        String head = headVar.toString() + ";";
        return graph.putRule(ruleId, body, head, graph.getMetaRuleInference());
    }

    /**
     * create reflexive rule R(from: X, to: X) :- R(from: X,to: Y)
     * @param ruleId rule identifier
     * @param relType reflexive relation type
     * @param graph graph
     * @return rule instance
     */
    public static Rule createReflexiveRule(String ruleId, RelationType relType, MindmapsGraph graph){
        final int arity = relType.hasRoles().size();
        if (arity != 2)
            throw new IllegalArgumentException(ErrorMessage.RULE_CREATION_ARITY_ERROR.getMessage());

        String body = Graql.var().isa(relType.getId()).rel("x").rel("y").toString() + ";";
        String head = Graql.var().isa(relType.getId()).rel("x").rel("x").toString() + ";";
        return graph.putRule(ruleId, body, head, graph.getMetaRuleInference());
    }

    /**
     * creates rule parent :- child
     * @param ruleId rule identifier
     * @param parent relation type of parent
     * @param child relation type of child
     * @param roleMappings map of corresponding role type ids
     * @param graph graph
     * @return rule instance
     */
    public static Rule createSubPropertyRule(String ruleId, RelationType parent, RelationType child, Map<String, String> roleMappings,
                                             MindmapsGraph graph){
        final int parentArity = parent.hasRoles().size();
        final int childArity = child.hasRoles().size();
        if (parentArity != childArity || parentArity != roleMappings.size())
            throw new IllegalArgumentException(ErrorMessage.RULE_CREATION_ARITY_ERROR.getMessage());
        Var parentVar = Graql.var().isa(parent.getId());
        Var childVar = Graql.var().isa(child.getId());
        Set<String> vars = new HashSet<>();

        roleMappings.forEach( (parentRoleId, childRoleId) -> {
            String varName = createFreshVariable(vars, "x");
            parentVar.rel(parentRoleId, varName);
            childVar.rel(childRoleId, varName);
            vars.add(varName);
        });
        String body = childVar.toString() + ";";
        String head = parentVar.toString() + ";";
        return graph.putRule(ruleId, body, head, graph.getMetaRuleInference());
    }

    public static Rule createPropertyChainRule(String ruleId, RelationType relation, String fromRoleId, String toRoleId,
                                             LinkedHashMap<RelationType, Pair<String, String>> chain, MindmapsGraph graph){
        Stack<String> varNames = new Stack<>();
        varNames.push("x");
        Set<VarAdmin> bodyVars = new HashSet<>();
        chain.forEach( (relType, rolePair) ->{
            String varName = createFreshVariable(Sets.newHashSet(varNames), "x");
            VarAdmin var = Graql.var().isa(relType.getId())
                    .rel(rolePair.getKey(), varNames.peek())
                    .rel(rolePair.getValue(), varName).admin();
            varNames.push(varName);
            bodyVars.add(var);
        });

        Var headVar = Graql.var().isa(relation.getId()).rel(fromRoleId, "x").rel(toRoleId, varNames.peek());
        String body = Patterns.conjunction(bodyVars).toString() + ";";
        String head = headVar.toString() + ";";
        return graph.putRule(ruleId, body, head, graph.getMetaRuleInference());
    }

    public static boolean checkTypesCompatible(Type aType, Type bType) {
        return aType.equals(bType) || aType.subTypes().contains(bType) || bType.subTypes().contains(aType);
    }
}
