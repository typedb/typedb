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
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.LabelProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.Predicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.query.UnifierImpl;
import ai.grakn.util.ErrorMessage;
import ai.grakn.util.Schema;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate.createValueVar;
import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Utiliy class providing useful functionalities.
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
                .flatMap(v -> v.hasProperty(LabelProperty.class)?
                        v.getProperties(LabelProperty.class).map(np -> new IdPredicate(typeVariable, np, parent)) :
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
            LabelProperty nameProp = typeVar.getProperty(LabelProperty.class).orElse(null);
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
     * get unifiers by comparing permutations with original variables
     * @param originalVars original ordered variables
     * @param permutations different permutations on the variables
     * @return set of unifiers
     */
    public static Set<Unifier> getUnifiersFromPermutations(List<VarName> originalVars, List<List<VarName>> permutations){
        Set<Unifier> unifierSet = new HashSet<>();
        permutations.forEach(perm -> {
            Unifier unifier = new UnifierImpl();
            Iterator<VarName> pIt = originalVars.iterator();
            Iterator<VarName> cIt = perm.iterator();
            while(pIt.hasNext() && cIt.hasNext()){
                VarName pVar = pIt.next();
                VarName chVar = cIt.next();
                if (!pVar.equals(chVar)) unifier.addMapping(pVar, chVar);
            }
            unifierSet.add(unifier);
        });
        return unifierSet;
    }

    /**
     * get all permutations of an entry list
     * @param entryList entry list to generate permutations of
     * @param <T> element type
     * @return set of all possible permutations
     */
    public static <T> List<List<T>> getListPermutations(List<T> entryList) {
        if (entryList.isEmpty()) {
            List<List<T>> result = new ArrayList<>();
            result.add(new ArrayList<>());
            return result;
        }
        List<T> list = new ArrayList<>(entryList);
        T firstElement = list.remove(0);
        List<List<T>> returnValue = new ArrayList<>();
        List<List<T>> permutations = getListPermutations(list);
        for (List<T> smallerPermuted : permutations) {
            for (int index = 0; index <= smallerPermuted.size(); index++) {
                List<T> temp = new ArrayList<>(smallerPermuted);
                temp.add(index, firstElement);
                returnValue.add(temp);
            }
        }
        return returnValue;
    }

    /**
     * @param role input role type
     * @return set of all non-meta super types of the role
     */
    public static Set<RoleType> getSuperTypes(RoleType role){
        Set<RoleType> superTypes = new HashSet<>();
        RoleType superRole = role.superType();
        while(!Schema.MetaSchema.isMetaLabel(superRole.getLabel())) {
            superTypes.add(superRole);
            superRole = superRole.superType();
        }
        return superTypes;
    }

    /**
     * @param roleTypes entry role type set
     * @return non-meta role types from within the provided set of role types
     */
    public static Set<RoleType> getTopRoles(Set<RoleType> roleTypes) {
        return roleTypes.stream()
                .filter(rt -> Sets.intersection(getSuperTypes(rt), roleTypes).isEmpty())
                .collect(toSet());
    }

    /**
     * Gets roletypes a given type can play in the provided relType relation type by performing
     * type intersection between type's playedRoles and relation's relates.
     * @param type for which we want to obtain compatible roles it plays
     * @param relRoles relation type of interest
     * @return set of role types the type can play in relType
     */
    public static Set<RoleType> getCompatibleRoleTypes(Type type, Set<RoleType> relRoles) {
        Collection<RoleType> typeRoles = type.plays();
        return relRoles.stream().filter(typeRoles::contains).collect(toSet());
    }

    /**
     * convert given role type to a set of relation types in which it can appear
     */
    public static final Function<RoleType, Set<RelationType>> roleToRelationTypes =
            role -> role.relationTypes().stream().filter(rt -> !rt.isImplicit()).collect(toSet());

    /**
     * convert given entity type to a set of relation types in which it can play roles
     */
    public static final Function<Type, Set<RelationType>> typeToRelationTypes =
            type -> type.plays().stream()
                    .flatMap(roleType -> roleType.relationTypes().stream())
                    .filter(rt -> !rt.isImplicit())
                    .collect(toSet());

    /**
     * compute the set of compatible relation types for given types (intersection of allowed sets of relation types for each entry type)
     * @param types for which the set of compatible relation types is to be computed
     * @param typeMapper function mapping a type to the set of compatible relation types
     * @param <T> type generic
     * @return set of compatible relation types
     */
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
    public static void computeRoleCombinations(Set<VarName> vars, Set<RoleType> roles, Map<VarName, Var> roleMap,
                                        Set<Map<VarName, Var>> roleMaps){
        Set<VarName> tempVars = Sets.newHashSet(vars);
        Set<RoleType> tempRoles = Sets.newHashSet(roles);
        VarName var = vars.iterator().next();

        roles.forEach(role -> {
            tempVars.remove(var);
            tempRoles.remove(role);
            roleMap.put(var, var().label(role.getLabel()).admin());
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
     * create transitive rule R(from: X, to: Y) :- R(from: X,to: Z), R(from: Z, to: Y)
     * @param relType transitive relation type
     * @param fromRoleLabel  from directional role type type label
     * @param toRoleLabel to directional role type type label
     * @param graph graph for the rule to be inserted
     * @return rule instance
     */
    public static Rule createTransitiveRule(RelationType relType, TypeLabel fromRoleLabel, TypeLabel toRoleLabel, GraknGraph graph){
        final int arity = relType.relates().size();
        if (arity != 2) throw new IllegalArgumentException(ErrorMessage.RULE_CREATION_ARITY_ERROR.getMessage());

        VarAdmin startVar = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), "z").admin();
        VarAdmin endVar = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "z").rel(Graql.label(toRoleLabel), "y").admin();
        VarAdmin headVar = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), "y").admin();
        Pattern body = Patterns.conjunction(Sets.newHashSet(startVar, endVar));
        return graph.admin().getMetaRuleInference().putRule(body, headVar);
    }

    /**
     * create reflexive rule R(from: X, to: X) :- R(from: X,to: Y)
     * @param relType reflexive relation type
     * @param fromRoleLabel from directional role type type label
     * @param toRoleLabel to directional role type type label
     * @param graph graph for the rule to be inserted
     * @return rule instance
     */
    public static Rule createReflexiveRule(RelationType relType, TypeLabel fromRoleLabel, TypeLabel toRoleLabel, GraknGraph graph){
        final int arity = relType.relates().size();
        if (arity != 2) throw new IllegalArgumentException(ErrorMessage.RULE_CREATION_ARITY_ERROR.getMessage());

        Var body = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), "y");
        Var head = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), "x");
        return graph.admin().getMetaRuleInference().putRule(body, head);
    }

    /**
     * creates rule parent :- child
     * @param parent relation type of parent
     * @param child relation type of child
     * @param roleMappings map of corresponding role type type names
     * @param graph graph for the rule to be inserted
     * @return rule instance
     */
    public static Rule createSubPropertyRule(RelationType parent, RelationType child, Map<TypeLabel, TypeLabel> roleMappings,
                                             GraknGraph graph){
        final int parentArity = parent.relates().size();
        final int childArity = child.relates().size();
        if (parentArity != childArity || parentArity != roleMappings.size()) {
            throw new IllegalArgumentException(ErrorMessage.RULE_CREATION_ARITY_ERROR.getMessage());
        }
        Var parentVar = var().isa(Graql.label(parent.getLabel()));
        Var childVar = var().isa(Graql.label(child.getLabel()));

        for (Map.Entry<TypeLabel, TypeLabel> entry : roleMappings.entrySet()) {
            VarName varName = VarName.anon();
            parentVar = parentVar.rel(Graql.label(entry.getKey()), var(varName));
            childVar = childVar.rel(Graql.label(entry.getValue()), var(varName));
        }
        return graph.admin().getMetaRuleInference().putRule(childVar, parentVar);
    }

    /**
     * creates rule R(fromRole: x, toRole: xm) :- R1(fromRole: x, ...), , R2, ... , Rn(..., toRole: xm)
     * @param relation head relation
     * @param fromRoleLabel specifies the role directionality of the head relation
     * @param toRoleLabel specifies the role directionality of the head relation
     * @param chain map containing ordered relation with their corresponding role mappings
     * @param graph graph for the rule to be inserted
     * @return rule instance
     */
    public static Rule createPropertyChainRule(RelationType relation, TypeLabel fromRoleLabel, TypeLabel toRoleLabel,
                                               LinkedHashMap<RelationType, Pair<TypeLabel, TypeLabel>> chain, GraknGraph graph){
        Stack<VarName> varNames = new Stack<>();
        varNames.push(VarName.of("x"));
        Set<VarAdmin> bodyVars = new HashSet<>();
        chain.forEach( (relType, rolePair) ->{
            VarName varName = VarName.anon();
            VarAdmin var = var().isa(Graql.label(relType.getLabel()))
                    .rel(Graql.label(rolePair.getKey()), var(varNames.peek()))
                    .rel(Graql.label(rolePair.getValue()), var(varName)).admin();
            varNames.push(varName);
            bodyVars.add(var);
        });

        Var headVar = var().isa(Graql.label(relation.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), var(varNames.peek()));
        return graph.admin().getMetaRuleInference().putRule(Patterns.conjunction(bodyVars), headVar);
    }
    
    /**
     * @param role in question
     * @return top non-meta super role of the role
     */
    public static RoleType getNonMetaTopRole(RoleType role){
        RoleType topRole = role;
        RoleType superRole = topRole.superType();
        while(!Schema.MetaSchema.isMetaLabel(superRole.getLabel())) {
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
}
