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
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.Unifier;
import grakn.core.graql.admin.UnifierComparison;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.internal.Schema;
import grakn.core.graql.internal.reasoner.atom.binary.TypeAtom;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.internal.reasoner.atom.predicate.ValuePredicate;
import grakn.core.graql.internal.reasoner.utils.conversion.RoleConverter;
import grakn.core.graql.internal.reasoner.utils.conversion.SchemaConceptConverter;
import grakn.core.graql.internal.reasoner.utils.conversion.TypeConverter;
import grakn.core.graql.query.pattern.Statement;
import grakn.core.graql.query.pattern.Variable;
import grakn.core.graql.query.pattern.property.IdProperty;
import grakn.core.graql.query.pattern.property.LabelProperty;
import grakn.core.graql.query.pattern.property.ValueProperty;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
                .flatMap(v -> v.hasProperty(LabelProperty.class)?
                        v.getProperties(LabelProperty.class).map(np -> IdPredicate.create(typeVariable, np.label(), parent)) :
                        v.getProperties(IdProperty.class).map(np -> IdPredicate.create(typeVariable, np.id(), parent)))
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
            LabelProperty nameProp = typeVar.getProperty(LabelProperty.class).orElse(null);
            if (nameProp != null) predicate = IdPredicate.create(typeVariable, nameProp.label(), parent);
        }
        return predicate;
    }

    /**
     * looks for appropriate var properties with a specified name among the vars and maps them to ValuePredicates,
     * covers both the case when variable is and isn't user defined
     * @param valueVariable variable name of interest
     * @param valueVar {@link Statement} to look for in case the variable name is not user defined
     * @param vars VarAdmins to look for properties
     * @param parent reasoner query the mapped predicate should belong to
     * @return stream of mapped ValuePredicates
     */
    public static Stream<ValuePredicate> getValuePredicates(Variable valueVariable, Statement valueVar, Set<Statement> vars, ReasonerQuery parent){
        Stream<Statement> sourceVars = valueVar.var().isUserDefinedName()?
                vars.stream().filter(v -> v.var().equals(valueVariable)).filter(v -> v.isPositive() == valueVar.isPositive()) :
                Stream.of(valueVar);
        return sourceVars.flatMap(v -> v.getProperties(ValueProperty.class).map(vp -> ValuePredicate.create(valueVariable, vp.predicate(), parent)));
    }

    /**
     * @param schemaConcept input type
     * @return set of all non-meta super types of the role
     */
    public static Set<SchemaConcept> supers(SchemaConcept schemaConcept){
        Set<SchemaConcept> superTypes = new HashSet<>();
        SchemaConcept superType = schemaConcept.sup();
        while(superType != null && !Schema.MetaSchema.isMetaLabel(superType.label())) {
            superTypes.add(superType);
            superType = superType.sup();
        }
        return superTypes;
    }

    /**
     * @param concept which hierarchy should be considered
     * @return set of {@link SchemaConcept}s consisting of the provided {@link SchemaConcept} and all its supers including meta
     */
    public static Set<SchemaConcept> upstreamHierarchy(SchemaConcept concept){
        Set<SchemaConcept> concepts = new HashSet<>();
        SchemaConcept superType = concept;
        while(superType != null) {
            concepts.add(superType);
            superType = superType.sup();
        }
        return concepts;
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
     * @param schemaConcepts entry {@link SchemaConcept} set
     * @return top non-meta {@link SchemaConcept}s from within the provided set
     */
    public static <T extends SchemaConcept> Set<T> top(Set<T> schemaConcepts) {
        return schemaConcepts.stream()
                .filter(rt -> Sets.intersection(supers(rt), schemaConcepts).isEmpty())
                .collect(toSet());
    }

    /**
     * @param schemaConcepts entry {@link SchemaConcept} set
     * @return bottom non-meta {@link SchemaConcept}s from within the provided set
     */
    public static <T extends SchemaConcept> Set<T> bottom(Set<T> schemaConcepts) {
        return schemaConcepts.stream()
                .filter(t -> Sets.intersection(t.subs().filter(t2 -> !t.equals(t2)).collect(toSet()), schemaConcepts).isEmpty())
                .collect(toSet());
    }

    /**
     * @param schemaConcepts entry {@link SchemaConcept} set
     * @return top {@link SchemaConcept}s from within the provided set or meta concept if it exists
     */
    public static <T extends SchemaConcept> Set<T> topOrMeta(Set<T> schemaConcepts) {
        Set<T> concepts = top(schemaConcepts);
        T meta = concepts.stream()
                .filter(c -> Schema.MetaSchema.isMetaLabel(c.label()))
                .findFirst().orElse(null);
        return meta != null ? Collections.singleton(meta) : concepts;
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
     * @param parent type
     * @param child type
     * @param direct flag indicating whether only direct types should be considered
     * @return true if child is a subtype of parent
     */
    public static boolean typesCompatible(SchemaConcept parent, SchemaConcept child, boolean direct) {
        if (parent == null ) return true;
        if (child == null) return false;
        if (direct) return parent.equals(child);
        if (Schema.MetaSchema.isMetaLabel(parent.label())) return true;
        SchemaConcept superType = child;
        while(superType != null && !Schema.MetaSchema.isMetaLabel(superType.label())){
            if (superType.equals(parent)) return true;
            superType = superType.sup();
        }
        return false;
    }

    /** determines disjointness of parent-child types, parent defines the bound on the child
     * @param parent {@link SchemaConcept}
     * @param child {@link SchemaConcept}
     * @return true if types do not belong to the same type hierarchy, also true if parent is null and false if parent non-null and child null
     */
    public static boolean areDisjointTypes(SchemaConcept parent, SchemaConcept child, boolean direct) {
        return parent != null && child == null || !typesCompatible(parent, child, direct) && !typesCompatible(child, parent, direct);
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
    public static <T> Collection<T> subtract(Collection<T> a, Collection<T> b){
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
