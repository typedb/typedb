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

package ai.grakn.graql.internal.reasoner.utils;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Label;
import ai.grakn.concept.OntologyConcept;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.Unifier;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import ai.grakn.graql.internal.pattern.property.IdProperty;
import ai.grakn.graql.internal.pattern.property.LabelProperty;
import ai.grakn.graql.internal.pattern.property.ValueProperty;
import ai.grakn.graql.internal.reasoner.UnifierImpl;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.reasoner.atom.predicate.ValuePredicate;
import ai.grakn.graql.internal.reasoner.utils.conversion.OntologyConceptConverter;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import javafx.util.Pair;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Stream;

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
public class ReasonerUtils {

    /**
     *
     * @param graph to be checked against
     * @return set of inference rule contained in the graph
     */
    public static Set<Rule> getRules(GraknGraph graph) {
        return graph.admin().getMetaRuleInference().instances().collect(toSet());
    }

    /**
     *
     * @param graph to be checked against
     * @return true if at least one inference rule is present in the graph
     */
    public static boolean hasRules(GraknGraph graph) {
        Label inferenceRule = Schema.MetaSchema.INFERENCE_RULE.getLabel();
        return graph.graql().infer(false).match(var("x").isa(Graql.label(inferenceRule))).ask().execute();
    }

    /**
     * looks for an appropriate var property with a specified name among the vars and maps it to an IdPredicate,
     * covers the case when specified variable name is user defined
     * @param typeVariable variable name of interest
     * @param vars VarAdmins to look for properties
     * @param parent reasoner query the mapped predicate should belong to
     * @return mapped IdPredicate
     */
    public static IdPredicate getUserDefinedIdPredicate(Var typeVariable, Set<VarPatternAdmin> vars, ReasonerQuery parent){
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
     * @param typeVar {@link VarPatternAdmin} to look for in case the variable name is not user defined
     * @param vars VarAdmins to look for properties
     * @param parent reasoner query the mapped predicate should belong to
     * @return mapped IdPredicate
     */
    @Nullable
    public static IdPredicate getIdPredicate(Var typeVariable, VarPatternAdmin typeVar, Set<VarPatternAdmin> vars, ReasonerQuery parent){
        IdPredicate predicate = null;
        //look for id predicate among vars
        if(typeVar.getVarName().isUserDefinedName()) {
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
     * @param valueVar {@link VarPatternAdmin} to look for in case the variable name is not user defined
     * @param vars VarAdmins to look for properties
     * @param parent reasoner query the mapped predicate should belong to
     * @return set of mapped ValuePredicates
     */
    public static Set<ValuePredicate> getValuePredicates(Var valueVariable, VarPatternAdmin valueVar, Set<VarPatternAdmin> vars, ReasonerQuery parent){
        Set<ValuePredicate> predicates = new HashSet<>();
        if(valueVar.getVarName().isUserDefinedName()){
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
     * get unifiers by comparing permutations with original variables
     * @param originalVars original ordered variables
     * @param permutations different permutations on the variables
     * @return set of unifiers
     */
    public static Set<Unifier> getUnifiersFromPermutations(List<Var> originalVars, List<List<Var>> permutations){
        Set<Unifier> unifierSet = new HashSet<>();
        permutations.forEach(perm -> {
            Unifier unifier = new UnifierImpl();
            Iterator<Var> pIt = originalVars.iterator();
            Iterator<Var> cIt = perm.iterator();
            while(pIt.hasNext() && cIt.hasNext()){
                Var pVar = pIt.next();
                Var chVar = cIt.next();
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
     * @param ontologyConcept input type
     * @return set of all non-meta super types of the role
     */
    public static Set<OntologyConcept> getSupers(OntologyConcept ontologyConcept){
        Set<OntologyConcept> superTypes = new HashSet<>();
        OntologyConcept superType = ontologyConcept.sup();
        while(!Schema.MetaSchema.isMetaLabel(superType.getLabel())) {
            superTypes.add(superType);
            superType = superType.sup();
        }
        return superTypes;
    }

    /**
     *
     * @param type for which top type is to be found
     * @return non-meta top type of the type
     */
    public static Type getTopType(Type type){
        Type superType = type;
        while(!Schema.MetaSchema.isMetaLabel(superType.getLabel())) {
            superType = superType.sup();
        }
        return superType;
    }

    /**
     * @param ontologyConcepts entry set
     * @return top non-meta {@link OntologyConcept} from within the provided set of {@link Role}
     */
    public static <T extends OntologyConcept> Set<T> getOntologyConcepts(Set<T> ontologyConcepts) {
        return ontologyConcepts.stream()
                .filter(rt -> Sets.intersection(getSupers(rt), ontologyConcepts).isEmpty())
                .collect(toSet());
    }

    /**
     * Gets roletypes a given type can play in the provided relType relation type by performing
     * type intersection between type's playedRoles and relation's relates.
     * @param type for which we want to obtain compatible roles it plays
     * @param relRoles relation type of interest
     * @return set of role types the type can play in relType
     */
    public static Set<Role> getCompatibleRoleTypes(Type type, Set<Role> relRoles) {
        Set<Role> typeRoles = type.plays().collect(toSet());
        return relRoles.stream().filter(typeRoles::contains).collect(toSet());
    }

    /**
     * calculates map intersection by doing an intersection on key sets and accumulating the keys
     * @param m1 first operand
     * @param m2 second operand
     * @param <K> map key type
     * @param <V> map value type
     * @return map intersection
     */
    public static <K, V> Multimap<K, V> multimapIntersection(Multimap<K, V> m1, Multimap<K, V> m2){
        Multimap<K, V> intersection = HashMultimap.create();
        Sets.SetView<K> keyIntersection = Sets.intersection(m1.keySet(), m2.keySet());
        Stream.concat(m1.entries().stream(), m2.entries().stream())
                .filter(e -> keyIntersection.contains(e.getKey()))
                .forEach(e -> intersection.put(e.getKey(), e.getValue()));
        return intersection;
    }

    /**
     * compute the map of compatible relation types for given types (intersection of allowed sets of relation types for each entry type)
     * and compatible role types
     * @param types for which the set of compatible relation types is to be computed
     //* @param typeMapper function mapping a type to the set of compatible relation types
     * @param <T> type generic
     * @return map of compatible relation types and their corresponding role types
     */
    public static <T extends OntologyConcept> Multimap<RelationType, Role> getCompatibleRelationTypesWithRoles(Set<T> types, OntologyConceptConverter<T> ontologyConceptConverter) {
        Multimap<RelationType, Role> compatibleTypes = HashMultimap.create();
        if (types.isEmpty()) return compatibleTypes;
        Iterator<T> it = types.iterator();
        compatibleTypes.putAll(ontologyConceptConverter.toRelationMultimap(it.next()));
        while(it.hasNext() && compatibleTypes.size() > 1) {
            compatibleTypes = multimapIntersection(compatibleTypes, ontologyConceptConverter.toRelationMultimap(it.next()));
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
    public static void computeRoleCombinations(Set<Var> vars, Set<Role> roles, Map<Var, VarPattern> roleMap,
                                               Set<Map<Var, VarPattern>> roleMaps){
        Set<Var> tempVars = Sets.newHashSet(vars);
        Set<Role> tempRoles = Sets.newHashSet(roles);
        Var var = vars.iterator().next();

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
    public static Rule createTransitiveRule(RelationType relType, Label fromRoleLabel, Label toRoleLabel, GraknGraph graph){
        if (!CommonUtil.containsOnly(relType.relates(), 2)) throw GraqlQueryException.ruleCreationArityMismatch();

        VarPatternAdmin startVar = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), "z").admin();
        VarPatternAdmin endVar = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "z").rel(Graql.label(toRoleLabel), "y").admin();
        VarPatternAdmin headVar = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), "y").admin();
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
    public static Rule createReflexiveRule(RelationType relType, Label fromRoleLabel, Label toRoleLabel, GraknGraph graph){
        if (!CommonUtil.containsOnly(relType.relates(), 2)) throw GraqlQueryException.ruleCreationArityMismatch();

        VarPattern body = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), "y");
        VarPattern head = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), "x");
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
    public static Rule createSubPropertyRule(RelationType parent, RelationType child, Map<Label, Label> roleMappings,
                                             GraknGraph graph){
        final long parentArity = parent.relates().count();
        final long childArity = child.relates().count();
        if (parentArity != childArity || parentArity != roleMappings.size()) {
            throw GraqlQueryException.ruleCreationArityMismatch();
        }
        VarPattern parentVar = var().isa(Graql.label(parent.getLabel()));
        VarPattern childVar = var().isa(Graql.label(child.getLabel()));

        for (Map.Entry<Label, Label> entry : roleMappings.entrySet()) {
            Var varName = var().asUserDefined();
            parentVar = parentVar.rel(Graql.label(entry.getKey()), varName);
            childVar = childVar.rel(Graql.label(entry.getValue()), varName);
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
    public static Rule createPropertyChainRule(RelationType relation, Label fromRoleLabel, Label toRoleLabel,
                                               LinkedHashMap<RelationType, Pair<Label, Label>> chain, GraknGraph graph){
        Stack<Var> varNames = new Stack<>();
        varNames.push(var("x"));
        Set<VarPatternAdmin> bodyVars = new HashSet<>();
        chain.forEach( (relType, rolePair) ->{
            Var varName = var().asUserDefined();
            VarPatternAdmin var = var().isa(Graql.label(relType.getLabel()))
                    .rel(Graql.label(rolePair.getKey()), varNames.peek())
                    .rel(Graql.label(rolePair.getValue()), varName).admin();
            varNames.push(varName);
            bodyVars.add(var);
        });

        VarPattern headVar = var().isa(Graql.label(relation.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), varNames.peek());
        return graph.admin().getMetaRuleInference().putRule(Patterns.conjunction(bodyVars), headVar);
    }

    /**
     * @param parent type
     * @param child type
     * @return true if child is a subtype of parent
     */
    public static boolean checkCompatible(OntologyConcept parent, OntologyConcept child) {
        if(Schema.MetaSchema.isMetaLabel(parent.getLabel())) return true;
        OntologyConcept superType = child;
        while(!Schema.MetaSchema.isMetaLabel(superType.getLabel())){
            if (superType.equals(parent)) return true;
            superType = superType.sup();
        }
        return false;
    }

    /**
     * @param parent type
     * @param child type
     * @return true if types do not belong to the same type hierarchy
     */
    public static boolean checkDisjoint(OntologyConcept parent, OntologyConcept child) {
        return !checkCompatible(parent, child) && !checkCompatible(child, parent);
    }
}
