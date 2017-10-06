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

import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
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
import ai.grakn.graql.internal.reasoner.utils.conversion.SchemaConceptConverter;
import ai.grakn.util.CommonUtil;
import ai.grakn.util.Schema;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.Collection;
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
     * looks for an appropriate var property with a specified name among the vars and maps it to an IdPredicate,
     * covers the case when specified variable name is user defined
     * @param typeVariable variable name of interest
     * @param vars VarAdmins to look for properties
     * @param parent reasoner query the mapped predicate should belong to
     * @return mapped IdPredicate
     */
    public static IdPredicate getUserDefinedIdPredicate(Var typeVariable, Set<VarPatternAdmin> vars, ReasonerQuery parent){
        return  vars.stream()
                .filter(v -> v.var().equals(typeVariable))
                .flatMap(v -> v.hasProperty(LabelProperty.class)?
                        v.getProperties(LabelProperty.class).map(np -> new IdPredicate(typeVariable, np.label(), parent)) :
                        v.getProperties(IdProperty.class).map(np -> new IdPredicate(typeVariable, np.id(), parent)))
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
        if(typeVar.var().isUserDefinedName()) {
            predicate = getUserDefinedIdPredicate(typeVariable, vars, parent);
        } else {
            LabelProperty nameProp = typeVar.getProperty(LabelProperty.class).orElse(null);
            if (nameProp != null) predicate = new IdPredicate(typeVariable, nameProp.label(), parent);
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
        if(valueVar.var().isUserDefinedName()){
            vars.stream()
                    .filter(v -> v.var().equals(valueVariable))
                    .flatMap(v -> v.getProperties(ValueProperty.class).map(vp -> new ValuePredicate(v.var(), vp.predicate(), parent)))
                    .forEach(predicates::add);
        }
        //add value atom
        else {
            valueVar.getProperties(ValueProperty.class)
                    .forEach(vp -> predicates
                            .add(new ValuePredicate(createValueVar(valueVariable, vp.predicate()), parent)));
        }
        return predicates;
    }

    /**
     * get unifiers by comparing permutations with original variables
     * @param originalVars original ordered variables
     * @param permutations different permutations on the variables
     * @return set of unifiers
     */
    public static Set<Unifier> getUnifiersFromPermutations(List<Pair<Var, Var>> originalVars, List<List<Pair<Var, Var>>> permutations){
        Set<Unifier> unifierSet = new HashSet<>();
        permutations.forEach(perm -> {
            Multimap<Var, Var> varMappings = HashMultimap.create();
            Iterator<Pair<Var, Var>> pIt = originalVars.iterator();
            Iterator<Pair<Var, Var>> cIt = perm.iterator();
            while(pIt.hasNext() && cIt.hasNext()){
                Pair<Var, Var> pPair = pIt.next();
                Pair<Var, Var> chPair = cIt.next();
                Var parentPlayer = pPair.getKey();
                Var childPlayer = chPair.getKey();
                Var parentRole = pPair.getValue();
                Var childRole = chPair.getValue();
                if (!parentPlayer.equals(childPlayer)) varMappings.put(parentPlayer, childPlayer);
                if (parentRole != null && childRole != null && !parentRole.equals(childRole)) varMappings.put(parentRole, childRole);
            }
            unifierSet.add(new UnifierImpl(varMappings));
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
     * @param schemaConcept input type
     * @return set of all non-meta super types of the role
     */
    public static Set<SchemaConcept> getSupers(SchemaConcept schemaConcept){
        Set<SchemaConcept> superTypes = new HashSet<>();
        SchemaConcept superType = schemaConcept.sup();
        while(superType != null && !Schema.MetaSchema.isMetaLabel(superType.getLabel())) {
            superTypes.add(superType);
            superType = superType.sup();
        }
        return superTypes;
    }

    /**
     * @param concept which hierarchy should be considered
     * @return set of {@link SchemaConcept}s: provided concept and all its supers including meta
     */
    public static Set<SchemaConcept> getUpstreamHierarchy(SchemaConcept concept){
        Set<SchemaConcept> concepts = new HashSet<>();
        SchemaConcept superType = concept;
        while(superType != null) {
            concepts.add(superType);
            superType = superType.sup();
        }
        return concepts;
    }

    /**
     *
     * @param type for which top type is to be found
     * @return non-meta top type of the type
     */
    public static Type getTopType(Type type){
        Type superType = type;
        while(superType != null && !Schema.MetaSchema.isMetaLabel(superType.getLabel())) {
            superType = superType.sup();
        }
        return superType;
    }

    /**
     * @param schemaConcepts entry set
     * @return top non-meta {@link SchemaConcept} from within the provided set of {@link Role}
     */
    public static <T extends SchemaConcept> Set<T> getSchemaConcepts(Set<T> schemaConcepts) {
        return schemaConcepts.stream()
                .filter(rt -> Sets.intersection(getSupers(rt), schemaConcepts).isEmpty())
                .collect(toSet());
    }

    /**
     * Gets roletypes a given type can play in the provided relType relation type by performing
     * type intersection between type's playedRoles and relation's relates.
     * @param type for which we want to obtain compatible roles it plays
     * @param relRoles relation type of interest
     * @return set of role types the type can play in relType
     */
    public static Set<Role> getCompatibleRoleTypes(Type type, Stream<Role> relRoles) {
        Set<Role> typeRoles = type.plays().collect(toSet());
        return relRoles.filter(typeRoles::contains).collect(toSet());
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
     * compute the map of compatible relation types for given types (intersection of allowed sets of relationship types for each entry type)
     * and compatible role types
     * @param types for which the set of compatible relation types is to be computed
     //* @param typeMapper function mapping a type to the set of compatible relation types
     * @param <T> type generic
     * @return map of compatible relation types and their corresponding role types
     */
    public static <T extends SchemaConcept> Multimap<RelationshipType, Role> getCompatibleRelationTypesWithRoles(Set<T> types, SchemaConceptConverter<T> schemaConceptConverter) {
        Multimap<RelationshipType, Role> compatibleTypes = HashMultimap.create();
        if (types.isEmpty()) return compatibleTypes;
        Iterator<T> it = types.iterator();
        compatibleTypes.putAll(schemaConceptConverter.toRelationshipMultimap(it.next()));
        while(it.hasNext() && compatibleTypes.size() > 1) {
            compatibleTypes = multimapIntersection(compatibleTypes, schemaConceptConverter.toRelationshipMultimap(it.next()));
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
     * create transitive {@link Rule} R(from: X, to: Y) :- R(from: X,to: Z), R(from: Z, to: Y)
     * @param label the {@link Label} of the new {@link Rule} to create
     * @param relType transitive {@link RelationshipType}
     * @param fromRoleLabel  from directional {@link Role} {@link Label}
     * @param toRoleLabel to directional {@link Role} {@link Label}
     * @param tx for the {@link Rule} to be inserted
     * @return the new {@link Rule}
     */
    public static Rule createTransitiveRule(String label, RelationshipType relType, Label fromRoleLabel, Label toRoleLabel, GraknTx tx){
        if (!CommonUtil.containsOnly(relType.relates(), 2)) throw GraqlQueryException.ruleCreationArityMismatch();

        VarPatternAdmin startVar = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), "z").admin();
        VarPatternAdmin endVar = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "z").rel(Graql.label(toRoleLabel), "y").admin();
        VarPatternAdmin headVar = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), "y").admin();
        Pattern body = Patterns.conjunction(Sets.newHashSet(startVar, endVar));
        return tx.putRule(label, body, headVar);
    }

    /**
     * create reflexive rule R(from: X, to: X) :- R(from: X,to: Y)
     * @param label the {@link Label} of the new {@link Rule} to create
     * @param relType reflexive {@link RelationshipType}
     * @param fromRoleLabel from directional {@link Role} {@link Label}
     * @param toRoleLabel to directional {@link Role} {@link Label}
     * @param tx for the {@link Rule} to be inserted
     * @return the new {@link Rule}
     */
    public static Rule createReflexiveRule(String label, RelationshipType relType, Label fromRoleLabel, Label toRoleLabel, GraknTx tx){
        if (!CommonUtil.containsOnly(relType.relates(), 2)) throw GraqlQueryException.ruleCreationArityMismatch();

        VarPattern body = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), "y");
        VarPattern head = var().isa(Graql.label(relType.getLabel())).rel(Graql.label(fromRoleLabel), "x").rel(Graql.label(toRoleLabel), "x");
        return tx.putRule(label, body, head);
    }

    /**
     * creates rule parent :- child
     * @param label the {@link Label} of the new {@link Rule} to create
     * @param parent {@link RelationshipType} of parent
     * @param child {@link RelationshipType} of child
     * @param roleMappings map of corresponding {@link Role} names
     * @param tx for the {@link Rule} to be inserted
     * @return the new {@link Rule}
     */
    public static Rule createSubPropertyRule(String label, RelationshipType parent, RelationshipType child, Map<Label, Label> roleMappings,
                                             GraknTx tx){
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
        return tx.putRule(label, childVar, parentVar);
    }

    /**
     * creates rule R(fromRole: x, toRole: xm) :- R1(fromRole: x, ...), , R2, ... , Rn(..., toRole: xm)
     * @param label the {@link Label} of the new {@link Rule} to create
     * @param relation head {@link RelationshipType}
     * @param fromRoleLabel specifies the {@link Role} directionality of the head {@link RelationshipType}
     * @param toRoleLabel specifies the {@link Role} directionality of the head {@link RelationshipType}
     * @param chain map containing ordered relation with their corresponding {@link Role} mappings
     * @param tx for the {@link Rule} to be inserted
     * @return the new {@link Rule}
     */
    public static Rule createPropertyChainRule(String label, RelationshipType relation, Label fromRoleLabel, Label toRoleLabel,
                                               LinkedHashMap<RelationshipType, Pair<Label, Label>> chain, GraknTx tx){
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
        return tx.putRule(label, Patterns.conjunction(bodyVars), headVar);
    }

    /**
     * @param parent type
     * @param child type
     * @return true if child is a subtype of parent
     */
    public static boolean typesCompatible(SchemaConcept parent, SchemaConcept child) {
        if (parent == null) return true;
        if (child == null) return false;
        if (Schema.MetaSchema.isMetaLabel(parent.getLabel())) return true;
        SchemaConcept superType = child;
        while(superType != null && !Schema.MetaSchema.isMetaLabel(superType.getLabel())){
            if (superType.equals(parent)) return true;
            superType = superType.sup();
        }
        return false;
    }

    /** determines disjointness of parent-child types, parent defines the bound on the child
     * @param parent type
     * @param child type
     * @return true if types do not belong to the same type hierarchy, also true if parent is null and false if parent non-null and child null
     */
    public static boolean areDisjointTypes(SchemaConcept parent, SchemaConcept child) {
        return parent != null && child == null || !typesCompatible(parent, child) && !typesCompatible(child, parent);
    }

    /**
     * @param a subtraction left operand
     * @param b subtraction right operand
     * @param <T> collection type
     * @return new Collection containing a minus a - b.
     * The cardinality of each element e in the returned Collection will be the cardinality of e in a minus the cardinality of e in b, or zero, whichever is greater.
     */
    public static <T> Collection<T> subtract(Collection<T> a, Collection<T> b){
        ArrayList<T> list = new ArrayList<>(a);
        for (T aC2 : b) {
            list.remove(aC2);
        }
        return list;
    }
}
