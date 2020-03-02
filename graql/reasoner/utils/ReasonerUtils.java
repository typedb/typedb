/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.graql.reasoner.utils;

import com.google.common.base.Equivalence;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import grakn.core.core.Schema;
import grakn.core.graql.reasoner.atom.PropertyAtomicFactory;
import grakn.core.graql.reasoner.atom.binary.TypeAtom;
import grakn.core.graql.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.reasoner.unifier.UnifierType;
import grakn.core.graql.reasoner.utils.conversion.RoleConverter;
import grakn.core.graql.reasoner.utils.conversion.SchemaConceptConverter;
import grakn.core.graql.reasoner.utils.conversion.TypeConverter;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.graql.reasoner.query.ReasonerQuery;
import grakn.core.kb.graql.reasoner.unifier.Unifier;
import graql.lang.property.IdProperty;
import graql.lang.property.TypeProperty;
import graql.lang.property.ValueProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Utility class providing useful functionalities.
 * </p>
 *
 *
 */
public class ReasonerUtils {

    public static SchemaConcept typeFromLabel(Label label, ConceptManager conceptManager) {
        SchemaConcept schemaConcept = conceptManager.getSchemaConcept(label);
        if (schemaConcept == null) throw GraqlSemanticException.labelNotFound(label);
        return schemaConcept;
    }

    /**
     * Looks for an appropriate var property with a specified name among the vars and maps it to a Label if possible.
     * Covers the case when specified variable name is user defined.
     * @param typeVariable variable name of interest
     * @param vars VarAdmins to look for properties
     * @return mapped IdPredicate
     */
    public static Label getLabelFromUserDefinedVar(Variable typeVariable, Set<Statement> vars, ConceptManager conceptManager){
        return  vars.stream()
                .filter(v -> v.var().equals(typeVariable))
                .flatMap(v -> {
                    if (v.hasProperty(TypeProperty.class)){
                        return v.getProperties(TypeProperty.class).map(np -> Label.of(np.name()));
                    }
                    return v.getProperties(IdProperty.class)
                            .map(np -> ConceptId.of(np.id()))
                            .map(conceptManager::<Concept>getConcept)
                            .filter(Objects::nonNull)
                            .map(t -> t.asSchemaConcept().label());
                })
                .findFirst().orElse(null);
    }

    /**
     * Looks for an appropriate var property with a specified name among the vars and maps it to a Label if possible.
     * Covers both the cases when variable is and isn't user defined.
     * @param typeVariable variable name of interest
     * @param typeVar Statement to look for in case the variable name is not user defined
     * @param vars VarAdmins to look for properties
     * @return mapped IdPredicate
     */
    @Nullable
    public static Label getLabel(Variable typeVariable, Statement typeVar, Set<Statement> vars, ConceptManager conceptManager){
        Label label = null;
        //look for id predicate among vars
        if(typeVar.var().isReturned()) {
            label = getLabelFromUserDefinedVar(typeVariable, vars, conceptManager);
        } else {
            TypeProperty nameProp = typeVar.getProperty(TypeProperty.class).orElse(null);
            if (nameProp != null){
                //NB: we do label conversion to make sure label is valid
                label = typeFromLabel(Label.of(nameProp.name()), conceptManager).label();
            }
        }
        return label;
    }

    /**
     * Finds a value predicate within a possible chain with a specified starting variable
     * @param var starting variable
     * @param otherStatements statement context
     * @return corresponding value predicate or null if not found
     */
    public static ValueProperty.Operation findValuePropertyOp(Variable var, Set<Statement> otherStatements){
        Variable[] searchVar = {null};
        ValueProperty.Operation[] predicate = {null};
        otherStatements.stream()
                .filter(s -> s.var().equals(var))
                .forEach(s ->
                        s.getProperties(ValueProperty.class)
                                .forEach(vp -> {
                                    Statement innerStatement = vp.operation().innerStatement();
                                    if (innerStatement != null){
                                        if (searchVar[0] != null) throw new IllegalStateException("Invalid variable to search for.");
                                        else searchVar[0] = innerStatement.var();
                                    } else {
                                        if (predicate[0] != null) throw new IllegalStateException("Invalid variable to search for.");
                                        else predicate[0] = vp.operation();
                                    }
                                })
                );
        return predicate[0] != null?
                predicate[0] :
                (searchVar[0] != null? findValuePropertyOp(searchVar[0], otherStatements) : null);
    }

    /**
     * looks for appropriate var properties with a specified name among the vars and maps them to ValuePredicates,
     * covers both the case when variable is and isn't user defined
     * @param valueVariable variable name of interest
     * @param statement Statement to look for in case the variable name is not user defined
     * @param fullContext VarAdmins to look for properties
     * @param parent reasoner query the mapped predicate should belong to
     * @return set of mapped ValuePredicates
     */
    public static Set<ValuePredicate> getValuePredicates(Variable valueVariable, Statement statement, Set<Statement> fullContext,
                                                         ReasonerQuery parent, PropertyAtomicFactory propertyAtomicFactory){
        Set<Statement> context = statement.var().isReturned()?
                fullContext.stream().filter(v -> v.var().equals(valueVariable)).collect(toSet()) :
                Collections.singleton(statement);

        return context.stream()
                .flatMap(s ->
                        s.getProperties(ValueProperty.class)
                        .map(property -> propertyAtomicFactory.value(property, parent, statement, fullContext)
                ))
                .filter(ValuePredicate.class::isInstance)
                .map(ValuePredicate.class::cast)
                .collect(toSet());
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
     * NB: assumes MATCH semantics - all types and their subs are considered
     * compute the map of compatible RelationTypes for a given set of Types
     * (intersection of allowed sets of relation types for each entry type) and compatible role types
     * @param types for which the set of compatible RelationTypes is to be computed
     * @param schemaConceptConverter converter between SchemaConcept and relation type-role entries
     * @param <T> type generic
     * @return map of compatible RelationTypes and their corresponding Roles
     */
    public static <T extends SchemaConcept> Multimap<RelationType, Role> compatibleRelationTypesWithRoles(Set<T> types, SchemaConceptConverter<T> schemaConceptConverter) {
        Multimap<RelationType, Role> compatibleTypes = HashMultimap.create();
        if (types.isEmpty()) return compatibleTypes;
        Iterator<T> typeIterator = types.iterator();
        compatibleTypes.putAll(schemaConceptConverter.toRelationMultimap(typeIterator.next()));

        while(typeIterator.hasNext() && compatibleTypes.size() > 1) {
            compatibleTypes = multimapIntersection(compatibleTypes, schemaConceptConverter.toRelationMultimap(typeIterator.next()));
        }
        return compatibleTypes;
    }

    /**
     * NB: assumes MATCH semantics - all types and their subs are considered
     * @param parentRole parent Role
     * @param parentType parent Type
     * @param entryRoles entry set of possible Roles
     * @return set of playable Roles defined by type-role parent combination, parent role assumed as possible
     */
    public static Set<Role> compatibleRoles(@Nullable Role parentRole, @Nullable Type parentType, Set<Role> entryRoles) {
        Set<Role> compatibleRoles = parentRole != null? Sets.newHashSet(parentRole) : Sets.newHashSet();

        if (parentRole != null && !Schema.MetaSchema.isMetaLabel(parentRole.label()) ){
            compatibleRoles.addAll(
                    Sets.intersection(
                            new RoleConverter().toCompatibleRoles(parentRole).collect(toSet()),
                            entryRoles
            ));
        } else {
            compatibleRoles.addAll(entryRoles);
        }

        if (parentType != null && !Schema.MetaSchema.isMetaLabel(parentType.label())) {
            Set<Role> compatibleRolesFromTypes = new TypeConverter().toCompatibleRoles(parentType).collect(toSet());

            //do set intersection meta role
            compatibleRoles = compatibleRoles.stream()
                    .filter(role -> Schema.MetaSchema.isMetaLabel(role.label()) || compatibleRolesFromTypes.contains(role))
                    .collect(toSet());
            //parent role also possible
            if (parentRole != null) compatibleRoles.add(parentRole);
        }
        return compatibleRoles;
    }

    public static Set<Role> compatibleRoles(Set<Type> types, Set<Role> relRoles){
        Iterator<Type> typeIterator = types.iterator();
        Set<Role> roles = relRoles;
        while(typeIterator.hasNext()){
            roles = Sets.intersection(roles, compatibleRoles(null, typeIterator.next(), relRoles));
        }
        return roles;
    }

    /**
     * @param childTypes type atoms of child query
     * @param parentTypes type atoms of parent query
     * @param childParentUnifier unifier to unify child with parent
     * @return combined unifier for type atoms
     */
    public static Unifier typeUnifier(Set<TypeAtom> childTypes, Set<TypeAtom> parentTypes, Unifier childParentUnifier, UnifierType unifierType){
        Unifier unifier = childParentUnifier;
        for(TypeAtom childType : childTypes){
            Variable childVarName = childType.getVarName();
            Variable parentVarName = childParentUnifier.containsKey(childVarName)? Iterables.getOnlyElement(childParentUnifier.get(childVarName)) : childVarName;

            //types are unique so getting one is fine
            TypeAtom parentType = parentTypes.stream().filter(pt -> pt.getVarName().equals(parentVarName)).findFirst().orElse(null);
            if (parentType != null){
                Unifier childUnifier = childType.getUnifier(parentType, unifierType);
                if (childUnifier != null) unifier = unifier.merge(childUnifier);
            }
        }
        return unifier;
    }

    /**
     * @param a first operand
     * @param b second operand
     * @param comparison function on the basis of which the collections shall be compared
     * @param <T> collection type
     * @return true iff the given collections contain equivalent elements with exactly the same cardinalities.
     */
    public static <T> boolean isEquivalentCollection(Collection<T> a, Collection<T> b, BiFunction<T, T, Boolean> comparison) {
        return a.size() == b.size()
                && a.stream().allMatch(e -> b.stream().anyMatch(e2 -> comparison.apply(e, e2)))
                && b.stream().allMatch(e -> a.stream().anyMatch(e2 -> comparison.apply(e, e2)));
    }

    private static <B, S extends B> Map<Equivalence.Wrapper<B>, Integer> getCardinalityMap(Collection<S> coll, Equivalence<B> equiv) {
        Map<Equivalence.Wrapper<B>, Integer> count = new HashMap<>();
        for (S obj : coll) count.merge(equiv.wrap(obj), 1, Integer::sum);
        return count;
    }

    /**
     * @param a first operand
     * @param b second operand
     * @param equiv equivalence on the basis of which the collections shall be compared
     * @param <B> collection base type
     * @param <S> collection super type
     * @return true iff the given Collections contain equivalent elements with exactly the same cardinalities.
     */
    public static <B, S extends B> boolean isEquivalentCollection(Collection<S> a, Collection<S> b,  Equivalence<B> equiv) {
        if (a.size() != b.size()) {
            return false;
        } else {
            Map<Equivalence.Wrapper<B>, Integer> mapA = getCardinalityMap(a, equiv);
            Map<Equivalence.Wrapper<B>, Integer> mapB = getCardinalityMap(b, equiv);
            if (mapA.size() != mapB.size()) {
                return false;
            } else {
                return mapA.keySet().stream().allMatch(k -> mapA.get(k).equals(mapB.get(k)));
            }
        }
    }

    /**
     * @param a subtraction left operand
     * @param b subtraction right operand
     * @param <T> collection type
     * @return new Collection containing a minus a - b.
     * The cardinality of each element e in the returned Collection will be the cardinality of e in a minus the cardinality of e in b, or zero, whichever is greater.
     */
    public static <T> List<T> listDifference(List<T> a, List<T> b){
        ArrayList<T> list = new ArrayList<>(a);
        b.forEach(list::remove);
        return list;
    }

    /**
     *
     * @param a union left operand
     * @param b union right operand
     * @param <T> list type
     * @return new list being a union of the two operands
     */
    public static <T> List<T> listUnion(List<T> a, List<T> b){
        List<T> union = new ArrayList<>(a);
        union.addAll(b);
        return union;
    }
}

