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

package ai.grakn.graql.internal.reasoner;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.NameProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import javafx.util.Pair;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;

import static ai.grakn.graql.Graql.name;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate.createValueVar;
import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Utiliy class providing useful
 * </p>
 *
 * @author Kasper Piskorski
 *
 */
public class Utility {

    private static final String CAPTURE_MARK = "captured-";

    /**
     * Capture a variable name, by prepending a constant to the name
     * @param var the variable name to capture
     * @return the captured variable
     */
    public static VarName capture(VarName var) {
        return var.map(CAPTURE_MARK::concat);
    }

    /**
     * Uncapture a variable name, by removing a prepended constant
     * @param var the variable name to uncapture
     * @return the uncaptured variable
     */
    public static VarName uncapture(VarName var) {
        // TODO: This could cause bugs if a user has a variable including the word "capture"
        return var.map(name -> name.replace(CAPTURE_MARK, ""));
    }

    /**
     * Check if a variable has been captured
     * @param var the variable to check
     * @return if the variable has been captured
     */
    public static boolean isCaptured(VarName var) {
        // TODO: This could cause bugs if a user has a variable including the word "capture"
        return var.getValue().contains(CAPTURE_MARK);
    }

    /**
     * looks for an appropriate var property with a specified name among the vars and maps it to an IdPredicate,
     * covers the case when specified variable name is user defined
     * @param typeVariable variable name of interest
     * @param vars VarAdmins to look for properties
     * @param parent reasoner query the mapped predicate should belong to
     * @return mapped IdPredicate
     */
    public static IdPredicate getUserDefinedIdPredicate(VarName typeVariable, Set<VarAdmin> vars, ReasonerQuery parent){
        return  vars.stream()
                .filter(v -> v.getVarName().equals(typeVariable))
                .flatMap(v -> v.hasProperty(NameProperty.class)?
                        v.getProperties(NameProperty.class).map(np -> new IdPredicate(typeVariable, np, parent)) :
                        v.getProperties(IdProperty.class).map(np -> new IdPredicate(typeVariable, np, parent)))
                .findFirst().orElse(null);
    }

    /**
     * looks for an appropriate var property with a specified name among the vars and maps it to an IdPredicate,
     * covers both the cases when variable is and isn't user defined
     * @param typeVariable variable name of interest
     * @param typeVar VarAdmin to look for in case the variable name is not user defined
     * @param vars VarAdmins to look for properties
     * @param parent reasoner query the mapped predicate should belong to
     * @return mapped IdPredicate
     */
    public static IdPredicate getIdPredicate(VarName typeVariable, VarAdmin typeVar, Set<VarAdmin> vars, ReasonerQuery parent){
        IdPredicate predicate = null;
        //look for id predicate among vars
        if(typeVar.isUserDefinedName()) {
            predicate = getUserDefinedIdPredicate(typeVariable, vars, parent);
        } else {
            NameProperty nameProp = typeVar.getProperty(NameProperty.class).orElse(null);
            if (nameProp != null) predicate = new IdPredicate(typeVariable, nameProp, parent);
        }
        return predicate;
    }

    /**
     * looks for appropriate var properties with a specified name among the vars and maps them to ValuePredicates,
     * covers both the case when variable is and isn't user defined
     * @param valueVariable variable name of interest
     * @param valueVar VarAdmin to look for in case the variable name is not user defined
     * @param vars VarAdmins to look for properties
     * @param parent reasoner query the mapped predicate should belong to
     * @return set of mapped ValuePredicates
     */
    public static Set<Predicate> getValuePredicates(VarName valueVariable, VarAdmin valueVar, Set<VarAdmin> vars, ReasonerQuery parent){
        Set<Predicate> predicates = new HashSet<>();
        if(valueVar.isUserDefinedName()){
            vars.stream()
                    .filter(v -> v.getVarName().equals(valueVariable))
                    .flatMap(v -> v.getProperties(ValueProperty.class).map(vp -> new ValuePredicate(v.getVarName(), vp.getPredicate(), parent)))
                    .forEach(predicates::add);
        }
        //add value atom
        else {
            valueVar.getProperties(ValueProperty.class)
                    .forEach(vp -> predicates
                            .add(new ValuePredicate(createValueVar(valueVariable, vp.getPredicate()), parent)));
        }
        return predicates;
    }

    /**
     * Provides more readable answer output.
     * @param answers set of answers to be printed
     */
    public static void printAnswers(Set<Map<String, Concept>> answers) {
        answers.forEach(result -> {
            result.entrySet().forEach(entry -> {
                Concept concept = entry.getValue();
                System.out.print(entry.getKey() + ": " + concept.getId() + " : ");
                if (concept.isResource()) {
                    System.out.print(concept.asResource().getValue() + " ");
                }
            });
            System.out.println();
        });
        System.out.println();
    }

    /**
     * Gets roletypes a given type can play in the provided relType relation type by performing
     * type intersection between type's playedRoles and relation's hasRoles.
     * @param type for which we want to obtain compatible roles it plays
     * @param relType relation type of interest
     * @return set of role types the type can play in relType
     */
    public static Set<RoleType> getCompatibleRoleTypes(Type type, Type relType) {
        Set<RoleType> cRoles = new HashSet<>();
        Collection<RoleType> typeRoles = type.playsRoles();
        Collection<RoleType> relRoles = ((RelationType) relType).hasRoles();
        relRoles.stream().filter(typeRoles::contains).forEach(cRoles::add);
        return cRoles;
    }

    public static final Function<RoleType, Set<RelationType>> roleToRelationTypes =
            role -> role.relationTypes().stream().filter(rt -> !rt.isImplicit()).collect(toSet());

    public static final Function<Type, Set<RelationType>> typeToRelationTypes =
            type -> type.playsRoles().stream()
                    .flatMap(roleType -> roleType.relationTypes().stream())
                    .filter(rt -> !rt.isImplicit())
                    .collect(toSet());

    public static <T extends Type> Set<RelationType> getCompatibleRelationTypes(Set<T> types, Function<T, Set<RelationType>> typeMapper) {
        Set<RelationType> compatibleTypes = new HashSet<>();
        if (types.isEmpty()) return compatibleTypes;
        Iterator<T> it = types.iterator();
        compatibleTypes.addAll(typeMapper.apply(it.next()));
        while(it.hasNext() && compatibleTypes.size() > 1) {
            compatibleTypes.retainAll(typeMapper.apply(it.next()));
        }
        return compatibleTypes;
    }

    /**
     * compute all rolePlayer-roleType combinations complementing provided roleMap
     * @param vars set of rolePlayers
     * @param roles set of roleTypes
     * @param roleMap initial rolePlayer-roleType roleMap to be complemented
     * @param roleMaps output set containing possible role mappings complementing the roleMap configuration
     */
    public static void computeRoleCombinations(Set<VarName> vars, Set<RoleType> roles, Map<VarName, VarAdmin> roleMap,
                                        Set<Map<VarName, Var>> roleMaps){
        Set<VarName> tempVars = Sets.newHashSet(vars);
        Set<RoleType> tempRoles = Sets.newHashSet(roles);
        VarName var = vars.iterator().next();

        roles.forEach(role -> {
            tempVars.remove(var);
            tempRoles.remove(role);
            roleMap.put(var, Graql.var().name(role.getName()).admin());
            if (!tempVars.isEmpty() && !tempRoles.isEmpty()) {
                computeRoleCombinations(tempVars, tempRoles, roleMap, roleMaps);
            } else {
                if (!roleMap.isEmpty()) {
                    roleMaps.add(Maps.newHashMap(roleMap));
                }
                roleMap.remove(var);
            }
            tempVars.add(var);
            tempRoles.add(role);
        });
    }

    /**
     * generate a fresh variable avoiding global variables and variables from the same query
     * @param vars  vars to be avoided
     * @param var variable to be generated a fresh replacement
     * @return fresh variables
     */
    public static VarName createFreshVariable(Set<VarName> vars, VarName var) {
        Set<String> names = vars.stream().map(VarName::getValue).collect(toSet());
        String fresh = var.getValue();
        while (names.contains(fresh)) {
            String valFree = fresh.replaceAll("[^0-9]", "");
            int value = valFree.equals("") ? 0 : Integer.parseInt(valFree);
            fresh = fresh.replaceAll("\\d+", "") + (++value);
        }
        return Patterns.varName(fresh);
    }

    /**
     * create transitive rule R(from: X, to: Y) :- R(from: X,to: Z), R(from: Z, to: Y)
     * @param relType transitive relation type
     * @param fromRoleName  from directional role type type name
     * @param toRoleName to directional role type type name
     * @param graph graph for the rule to be inserted
     * @return rule instance
     */
    public static Rule createTransitiveRule(RelationType relType, TypeName fromRoleName, TypeName toRoleName, GraknGraph graph){
        final int arity = relType.hasRoles().size();
        if (arity != 2) throw new IllegalArgumentException(ErrorMessage.RULE_CREATION_ARITY_ERROR.getMessage());

        VarAdmin startVar = var().isa(name(relType.getName())).rel(name(fromRoleName), "x").rel(name(toRoleName), "z").admin();
        VarAdmin endVar = var().isa(name(relType.getName())).rel(name(fromRoleName), "z").rel(name(toRoleName), "y").admin();
        VarAdmin headVar = var().isa(name(relType.getName())).rel(name(fromRoleName), "x").rel(name(toRoleName), "y").admin();
        Pattern body = Patterns.conjunction(Sets.newHashSet(startVar, endVar));
        return graph.admin().getMetaRuleInference().addRule(body, headVar);
    }

    /**
     * create reflexive rule R(from: X, to: X) :- R(from: X,to: Y)
     * @param relType reflexive relation type
     * @param graph graph for the rule to be inserted
     * @return rule instance
     */
    public static Rule createReflexiveRule(RelationType relType, GraknGraph graph){
        final int arity = relType.hasRoles().size();
        if (arity != 2) throw new IllegalArgumentException(ErrorMessage.RULE_CREATION_ARITY_ERROR.getMessage());

        Var body = var().isa(name(relType.getName())).rel("x").rel("y");
        Var head = var().isa(name(relType.getName())).rel("x").rel("x");
        return graph.admin().getMetaRuleInference().addRule(body, head);
    }

    /**
     * creates rule parent :- child
     * @param parent relation type of parent
     * @param child relation type of child
     * @param roleMappings map of corresponding role type type names
     * @param graph graph for the rule to be inserted
     * @return rule instance
     */
    public static Rule createSubPropertyRule(RelationType parent, RelationType child, Map<TypeName, TypeName> roleMappings,
                                             GraknGraph graph){
        final int parentArity = parent.hasRoles().size();
        final int childArity = child.hasRoles().size();
        if (parentArity != childArity || parentArity != roleMappings.size()) {
            throw new IllegalArgumentException(ErrorMessage.RULE_CREATION_ARITY_ERROR.getMessage());
        }
        Var parentVar = var().isa(name(parent.getName()));
        Var childVar = var().isa(name(child.getName()));
        Set<VarName> vars = new HashSet<>();

        roleMappings.forEach( (parentRoleName, childRoleName) -> {
            VarName varName = createFreshVariable(vars, Patterns.varName("x"));
            parentVar.rel(name(parentRoleName), var(varName));
            childVar.rel(name(childRoleName), var(varName));
            vars.add(varName);
        });
        return graph.admin().getMetaRuleInference().addRule(childVar, parentVar);
    }

    /**
     * creates rule R(fromRole: x, toRole: xm) :- R1(fromRole: x, ...), , R2, ... , Rn(..., toRole: xm)
     * @param relation head relation
     * @param fromRoleName specifies the role directionality of the head relation
     * @param toRoleName specifies the role directionality of the head relation
     * @param chain map containing ordered relation with their corresponding role mappings
     * @param graph graph for the rule to be inserted
     * @return rule instance
     */
    public static Rule createPropertyChainRule(RelationType relation, TypeName fromRoleName, TypeName toRoleName,
                                             LinkedHashMap<RelationType, Pair<TypeName, TypeName>> chain, GraknGraph graph){
        Stack<VarName> varNames = new Stack<>();
        varNames.push(Patterns.varName("x"));
        Set<VarAdmin> bodyVars = new HashSet<>();
        chain.forEach( (relType, rolePair) ->{
            VarName varName = createFreshVariable(Sets.newHashSet(varNames), Patterns.varName("x"));
            VarAdmin var = var().isa(name(relType.getName()))
                    .rel(name(rolePair.getKey()), var(varNames.peek()))
                    .rel(name(rolePair.getValue()), var(varName)).admin();
            varNames.push(varName);
            bodyVars.add(var);
        });

        Var headVar = var().isa(name(relation.getName())).rel(name(fromRoleName), "x").rel(name(toRoleName), var(varNames.peek()));
        return graph.admin().getMetaRuleInference().addRule(Patterns.conjunction(bodyVars), headVar);
    }

    /**
     * For a given role returns all its non-meta super roles.
     * @param role in question
     * @return set of role's non-meta super types
     */
    public static Set<RoleType> getNonMetaSuperRoleTypes(RoleType role){
        Set<RoleType> roles = new HashSet<>();
        RoleType baseRole = role.superType();
        while(!Schema.MetaSchema.isMetaName(baseRole.getName())){
            roles.add(baseRole);
            baseRole = baseRole.superType();
        }
        return roles;
    }

    /**
     * @param role in question
     * @return top non-meta super role of the role
     */
    public static RoleType getNonMetaTopRole(RoleType role){
        RoleType topRole = role;
        RoleType superRole = topRole.superType();
        while(!Schema.MetaSchema.isMetaName(superRole.getName())) {
            topRole = superRole;
            superRole = superRole.superType();
        }
        return topRole;
    }

    /**
     * @param parent type
     * @param child type
     * @return true if child is subtype of parent
     */
    public static boolean checkTypesCompatible(Type parent, Type child) {
        return parent.equals(child) || parent.subTypes().contains(child);
    }

    /**
     * @param A set
     * @param B set
     * @param <T> set element type
     * @return results of set subtraction starting with larger set
     */
    public static <T> Set<T> subtractSets(Set<T> A, Set<T> B){
        Set<T> sub =  A.size() > B.size()? Sets.newHashSet(A) : Sets.newHashSet(B);
        if (A.size() > B.size()) {
            sub.removeAll(B);
        } else {
            sub.removeAll(A);
        }
        return sub;
    }
}
