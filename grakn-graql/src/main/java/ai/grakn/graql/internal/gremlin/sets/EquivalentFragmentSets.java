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

import ai.grakn.GraknGraph;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;

import java.util.Collection;
import java.util.Optional;

import static ai.grakn.graql.internal.gremlin.sets.ResourceIndexFragmentSet.applyResourceIndexOptimisation;

/**
 * Factory class for producing instances of {@link EquivalentFragmentSet}.
 *
 * @author Felix Chapman
 */
public class EquivalentFragmentSets {

    /**
     * An {@link EquivalentFragmentSet} that indicates two castings are unique
     */
    public static EquivalentFragmentSet distinctCasting(VarName castingA, VarName castingB) {
        return new DistinctCastingFragmentSet(castingA, castingB);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a type whose instances play a role.
     * @param type a type variable label
     * @param roleType a role type variable label
     * @param required whether the plays must be constrained to be "required"
     */
    public static EquivalentFragmentSet plays(VarName type, VarName roleType, boolean required) {
        return new PlaysFragmentSet(type, roleType, required);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a relation with a casting.
     */
    public static EquivalentFragmentSet casting(VarName relation, VarName casting) {
        return new CastingFragmentSet(relation, casting);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a casting connected to a role-player.
     */
    public static EquivalentFragmentSet rolePlayer(VarName casting, VarName rolePlayer) {
        return new RolePlayerFragmentSet(casting, rolePlayer);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is an instance of a role-type.
     */
    public static EquivalentFragmentSet isaCastings(VarName casting, VarName roleType) {
        return new IsaCastingsFragmentSet(casting, roleType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a shortcut edge between two role-players.
     * @param roleTypeA an optional role-type for {@param rolePlayerA}
     * @param rolePlayerA a role-player variable label
     * @param roleTypeB an optional role-type for {@param rolePlayerB}
     * @param rolePlayerB a role-player variable label
     * @param relationType an optional relation-type for the shortcut
     */
    public static EquivalentFragmentSet shortcut(
            Optional<TypeLabel> roleTypeA, VarName rolePlayerA,
            Optional<TypeLabel> roleTypeB, VarName rolePlayerB, Optional<TypeLabel> relationType) {
        return new ShortcutFragmentSet(roleTypeA, rolePlayerA, roleTypeB, rolePlayerB, relationType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a sub-type of another variable.
     */
    public static EquivalentFragmentSet sub(VarName subType, VarName superType) {
        return new SubFragmentSet(subType, superType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a relation with a scope.
     */
    public static EquivalentFragmentSet hasScope(VarName relation, VarName scope) {
        return new HasScopeFragmentSet(relation, scope);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a relation type which involves a role.
     */
    public static EquivalentFragmentSet relates(VarName relationType, VarName roleType) {
        return new RelatesFragmentSet(relationType, roleType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is not a casting.
     */
    public static EquivalentFragmentSet notCasting(VarName start) {
        return new NotCastingFragmentSet(start);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is an instance of a type.
     */
    public static EquivalentFragmentSet isa(VarName instance, VarName type) {
        return new IsaFragmentSet(instance, type);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is not equal to another variable.
     */
    public static EquivalentFragmentSet neq(VarName varA, VarName varB) {
        return new NeqFragmentSet(varA, varB);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable represents a resource with value matching a predicate.
     */
    public static EquivalentFragmentSet value(VarName resource, ValuePredicateAdmin predicate) {
        return new ValueFragmentSet(resource, predicate);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a concept with a particular ID.
     */
    public static EquivalentFragmentSet id(VarName start, ConceptId id) {
        return new IdFragmentSet(start, id);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable represents an abstract type.
     */
    public static EquivalentFragmentSet isAbstract(VarName start) {
        return new IsAbstractFragmentSet(start);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a type with a particular label.
     */
    public static EquivalentFragmentSet label(VarName type, TypeLabel label) {
        return new LabelFragmentSet(type, label);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a resource type with a data-type.
     */
    public static EquivalentFragmentSet dataType(VarName resourceType, ResourceType.DataType<?> dataType) {
        return new DataTypeFragmentSet(resourceType, dataType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a resource type whose instances must conform to a given regex.
     */
    public static EquivalentFragmentSet regex(VarName resourceType, String regex) {
        return new RegexFragmentSet(resourceType, regex);
    }

    // TODO: Move shortcut edge optimisation here
    /**
     * Modify the given collection of {@link EquivalentFragmentSet} to introduce certain optimisations, such as the
     * {@link ResourceIndexFragmentSet}.
     *
     * This involves substituting various {@link EquivalentFragmentSet} with other {@link EquivalentFragmentSet}.
     */
    public static void optimiseFragmentSets(
            Collection<EquivalentFragmentSet> fragmentSets, GraknGraph graph) {

        // Repeatedly apply optimisations until they don't alter the query
        boolean changed = true;

        while (changed) {
            changed = applyResourceIndexOptimisation(fragmentSets, graph);
        }
    }
}
