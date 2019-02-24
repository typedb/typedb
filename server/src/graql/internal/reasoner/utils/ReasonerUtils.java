/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.internal.reasoner.utils;

import com.google.common.base.Equivalence;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.concept.type.SchemaConcept;
import grakn.core.concept.type.Type;
import grakn.core.graql.internal.reasoner.atom.AtomicFactory;
import grakn.core.graql.internal.reasoner.atom.binary.TypeAtom;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.internal.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.internal.reasoner.query.ReasonerQuery;
import grakn.core.graql.internal.reasoner.unifier.Unifier;
import grakn.core.graql.internal.reasoner.unifier.UnifierComparison;
import grakn.core.graql.internal.reasoner.utils.conversion.RoleConverter;
import grakn.core.graql.internal.reasoner.utils.conversion.SchemaConceptConverter;
import grakn.core.graql.internal.reasoner.utils.conversion.TypeConverter;
import grakn.core.server.kb.Schema;
import graql.lang.property.IdProperty;
import graql.lang.property.TypeProperty;
import graql.lang.property.ValueProperty;
import graql.lang.statement.Statement;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 *
 * <p>
 * Utiliy class providing useful functionalities.
 * </p>
 *
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
    public static IdPredicate getUserDefinedIdPredicate(Variable typeVariable, Set<Statement> vars, ReasonerQuery parent){
        return  vars.stream()
                .filter(v -> v.var().equals(typeVariable))
                .flatMap(v -> v.hasProperty(TypeProperty.class)?
                        v.getProperties(TypeProperty.class).map(np -> IdPredicate.create(typeVariable, Label.of(np.name()), parent)) :
                        v.getProperties(IdProperty.class).map(np -> IdPredicate.create(typeVariable, ConceptId.of(np.id()), parent)))
                .findFirst().orElse(null);
    }

    /**
     * looks for an appropriate var property with a specified name among the vars and maps it to an IdPredicate,
     * covers both the cases when variable is and isn't user defined
     * @param typeVariable variable name of interest
     * @param typeVar {@link Statement} to look for in case the variable name is not user defined
     * @param vars VarAdmins to look for properties
     * @param parent reasoner query the mapped predicate should belong to
     * @return mapped IdPredicate
     */
    @Nullable
    public static IdPredicate getIdPredicate(Variable typeVariable, Statement typeVar, Set<Statement> vars, ReasonerQuery parent){
        IdPredicate predicate = null;
        //look for id predicate among vars
        if(typeVar.var().isUserDefinedName()) {
            predicate = getUserDefinedIdPredicate(typeVariable, vars, parent);
        } else {
            TypeProperty nameProp = typeVar.getProperty(TypeProperty.class).orElse(null);
            if (nameProp != null) predicate = IdPredicate.create(typeVariable, Label.of(nameProp.name()), parent);
        }
        return predicate;
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
                                        if (searchVar[0] != null) throw new IllegalStateException("bla");
                                        else searchVar[0] = innerStatement.var();
                                    } else {
                                        if (predicate[0] != null) throw new IllegalStateException("bla");
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
     * @param statement {@link Statement} to look for in case the variable name is not user defined
     * @param fullContext VarAdmins to look for properties
     * @param parent reasoner query the mapped predicate should belong to
     * @return stream of mapped ValuePredicates
     */
    public static Stream<ValuePredicate> getValuePredicates(Variable valueVariable, Statement statement, Set<Statement> fullContext, ReasonerQuery parent){
        Stream<Statement> context = statement.var().isUserDefinedName()?
                fullContext.stream().filter(v -> v.var().equals(valueVariable)) :
                Stream.of(statement);
        Set<ValuePredicate> vps = context
                .flatMap(v -> v.getProperties(ValueProperty.class)
                        .map(property -> AtomicFactory.createValuePredicate(property, statement, fullContext, false, false, parent))
                        .filter(ValuePredicate.class::isInstance)
                        .map(ValuePredicate.class::cast)
                )
                .collect(toSet());
        return vps.stream();
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
     * compute the map of compatible {@link RelationType}s for a given set of {@link Type}s
     * (intersection of allowed sets of relation types for each entry type) and compatible role types
     * @param types for which the set of compatible {@link RelationType}s is to be computed
     * @param schemaConceptConverter converter between {@link SchemaConcept} and relation type-role entries
     * @param <T> type generic
     * @return map of compatible {@link RelationType}s and their corresponding {@link Role}s
     */
    public static <T extends SchemaConcept> Multimap<RelationType, Role> compatibleRelationTypesWithRoles(Set<T> types, SchemaConceptConverter<T> schemaConceptConverter) {
        Multimap<RelationType, Role> compatibleTypes = HashMultimap.create();
        if (types.isEmpty()) return compatibleTypes;
        Iterator<T> typeIterator = types.iterator();
        compatibleTypes.putAll(schemaConceptConverter.toRelationshipMultimap(typeIterator.next()));

        while(typeIterator.hasNext() && compatibleTypes.size() > 1) {
            compatibleTypes = multimapIntersection(compatibleTypes, schemaConceptConverter.toRelationshipMultimap(typeIterator.next()));
        }
        return compatibleTypes;
    }

    /**
     * NB: assumes MATCH semantics - all types and their subs are considered
     * @param parentRole parent {@link Role}
     * @param parentType parent {@link Type}
     * @param entryRoles entry set of possible {@link Role}s
     * @return set of playable {@link Role}s defined by type-role parent combination, parent role assumed as possible
     */
    public static Set<Role> compatibleRoles(Role parentRole, Type parentType, Set<Role> entryRoles) {
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

    public static Set<Role> compatibleRoles(Type type, Set<Role> relRoles){
        return compatibleRoles(null, type, relRoles);
    }


    /**
     * @param childTypes type atoms of child query
     * @param parentTypes type atoms of parent query
     * @param childParentUnifier unifier to unify child with parent
     * @return combined unifier for type atoms
     */
    public static Unifier typeUnifier(Set<TypeAtom> childTypes, Set<TypeAtom> parentTypes, Unifier childParentUnifier, UnifierComparison unifierType){
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
        for (S obj : coll) count.merge(equiv.wrap(obj), 1, (a, b) -> a + b);
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
