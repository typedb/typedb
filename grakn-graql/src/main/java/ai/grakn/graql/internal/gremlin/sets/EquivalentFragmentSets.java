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
 *
 */

package ai.grakn.graql.internal.gremlin.sets;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeName;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;

import java.util.Optional;

import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inCasting;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inIsaCastings;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.inRolePlayer;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outCasting;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outIsaCastings;
import static ai.grakn.graql.internal.gremlin.fragment.Fragments.outRolePlayer;

public class EquivalentFragmentSets {

    /**
     * An {@link EquivalentFragmentSet} that indicates two castings are unique
     */
    public static EquivalentFragmentSet distinctCasting(VarName castingA, VarName castingB) {
        return EquivalentFragmentSet.create(
                Fragments.distinctCasting(castingA, castingB),
                Fragments.distinctCasting(castingB, castingA)
        );
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a type whose instances play a role.
     * @param type a type variable name
     * @param roleType a role type variable name
     * @param required whether the plays-role must be constrained to be "required"
     */
    public static EquivalentFragmentSet playsRole(VarName type, VarName roleType, boolean required) {
        return EquivalentFragmentSet.create(
                Fragments.outPlaysRole(type, roleType, required),
                Fragments.inPlaysRole(roleType, type, required)
        );
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a relation with a casting.
     */
    public static EquivalentFragmentSet casting(VarName relation, VarName casting) {
        return EquivalentFragmentSet.create(outCasting(relation, casting), inCasting(casting, relation));
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a casting connected to a role-player.
     */
    public static EquivalentFragmentSet rolePlayer(VarName casting, VarName rolePlayer) {
        return EquivalentFragmentSet.create(outRolePlayer(casting, rolePlayer), inRolePlayer(rolePlayer, casting));
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is an instance of a role-type.
     */
    public static EquivalentFragmentSet isaCastings(VarName casting, VarName roleType) {
        return EquivalentFragmentSet.create(
                outIsaCastings(casting, roleType), inIsaCastings(roleType, casting)
        );
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a shortcut edge between two role-players.
     * @param roleTypeA an optional role-type for {@param rolePlayerA}
     * @param rolePlayerA a role-player variable name
     * @param roleTypeB an optional role-type for {@param rolePlayerB}
     * @param rolePlayerB a role-player variable name
     * @param relationType an optional relation-type for the shortcut
     */
    public static EquivalentFragmentSet shortcut(
            Optional<TypeName> roleTypeA, VarName rolePlayerA,
            Optional<TypeName> roleTypeB,VarName rolePlayerB, Optional<TypeName> relationType) {
        return EquivalentFragmentSet.create(
                Fragments.shortcut(relationType, roleTypeA, roleTypeB, rolePlayerA, rolePlayerB),
                Fragments.shortcut(relationType, roleTypeB, roleTypeA, rolePlayerB, rolePlayerA)
        );
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a sub-type of another variable.
     */
    public static EquivalentFragmentSet sub(VarName subType, VarName superType) {
        return EquivalentFragmentSet.create(
                Fragments.outSub(subType, superType),
                Fragments.inSub(superType, subType)
        );
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a relation with a scope.
     */
    public static EquivalentFragmentSet hasScope(VarName relation, VarName scope) {
        return EquivalentFragmentSet.create(
                Fragments.outHasScope(relation, scope),
                Fragments.inHasScope(scope, relation)
        );
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a relation type which involves a role.
     */
    public static EquivalentFragmentSet hasRole(VarName relationType, VarName roleType) {
        return EquivalentFragmentSet.create(
                Fragments.outHasRole(relationType, roleType),
                Fragments.inHasRole(roleType, relationType)
        );
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is not a casting.
     */
    public static EquivalentFragmentSet notCasting(VarName start) {
        return EquivalentFragmentSet.create(Fragments.notCasting(start));
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is an instance of a type.
     */
    public static EquivalentFragmentSet isa(VarName instance, VarName type) {
        return EquivalentFragmentSet.create(
                Fragments.outIsa(instance, type),
                Fragments.inIsa(type, instance)
        );
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is not equal to another variable.
     */
    public static EquivalentFragmentSet neq(VarName varA, VarName varB) {
        return EquivalentFragmentSet.create(Fragments.neq(varA, varB), Fragments.neq(varB, varA));
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable represents a resource with value matching a predicate.
     */
    public static EquivalentFragmentSet value(VarName resource, ValuePredicateAdmin predicate) {
        return EquivalentFragmentSet.create(Fragments.value(resource, predicate));
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a concept with a particular ID.
     */
    public static EquivalentFragmentSet id(VarName start, ConceptId id) {
        return EquivalentFragmentSet.create(Fragments.id(start, id));
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable represents an abstract type.
     */
    public static EquivalentFragmentSet isAbstract(VarName start) {
        return EquivalentFragmentSet.create(Fragments.isAbstract(start));
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a type with a particular name.
     */
    public static EquivalentFragmentSet name(VarName type, TypeName name) {
        return EquivalentFragmentSet.create(Fragments.name(type, name));
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a resource type with a data-type.
     */
    public static EquivalentFragmentSet dataType(VarName resourceType, ResourceType.DataType<?> dataType) {
        return EquivalentFragmentSet.create(Fragments.dataType(resourceType, dataType));
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a resource type whose instances must conform to a given regex.
     */
    public static EquivalentFragmentSet regex(VarName resourceType, String regex) {
        return EquivalentFragmentSet.create(Fragments.regex(resourceType, regex));
    }
}
