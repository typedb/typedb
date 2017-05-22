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
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.ValuePredicateAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.gremlin.sets.ResourceIndexFragmentSet.applyResourceIndexOptimisation;
import static ai.grakn.graql.internal.gremlin.sets.ShortcutFragmentSet.applyShortcutOptimisation;

/**
 * Factory class for producing instances of {@link EquivalentFragmentSet}.
 *
 * @author Felix Chapman
 */
public class EquivalentFragmentSets {

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a type whose instances play a role.
     * @param type a type variable label
     * @param roleType a role type variable label
     * @param required whether the plays must be constrained to be "required"
     */
    public static EquivalentFragmentSet plays(Var type, Var roleType, boolean required) {
        return new PlaysFragmentSet(type, roleType, required);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a relation with a casting.
     */
    public static EquivalentFragmentSet casting(Var relation, Var casting) {
        return new CastingFragmentSet(relation, casting);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a casting connected to a role-player.
     */
    public static EquivalentFragmentSet rolePlayer(Var casting, Var rolePlayer) {
        return new RolePlayerFragmentSet(casting, rolePlayer);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is an instance of a role-type.
     */
    public static EquivalentFragmentSet isaCastings(Var casting, Var roleType) {
        return new IsaCastingsFragmentSet(casting, roleType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a shortcut edge between two role-players.
     */
    public static EquivalentFragmentSet shortcut(Var relation, Var edge, Var rolePlayer) {
        return new ShortcutFragmentSet(relation, edge, rolePlayer, null, null);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a sub-type of another variable.
     */
    public static EquivalentFragmentSet sub(Var subType, Var superType) {
        return new SubFragmentSet(subType, superType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a relation with a scope.
     */
    public static EquivalentFragmentSet hasScope(Var relation, Var scope) {
        return new HasScopeFragmentSet(relation, scope);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a relation type which involves a role.
     */
    public static EquivalentFragmentSet relates(Var relationType, Var roleType) {
        return new RelatesFragmentSet(relationType, roleType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is not a casting or a shard.
     */
    public static EquivalentFragmentSet notInternalFragmentSet(Var start) {
        return new NotInternalFragmentSet(start);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is an instance of a type.
     */
    public static EquivalentFragmentSet isa(Var instance, Var type) {
        return new IsaFragmentSet(instance, type);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is not equal to another variable.
     */
    public static EquivalentFragmentSet neq(Var varA, Var varB) {
        return new NeqFragmentSet(varA, varB);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable represents a resource with value matching a predicate.
     */
    public static EquivalentFragmentSet value(Var resource, ValuePredicateAdmin predicate) {
        return new ValueFragmentSet(resource, predicate);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a concept with a particular ID.
     */
    public static EquivalentFragmentSet id(Var start, ConceptId id) {
        return new IdFragmentSet(start, id);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable represents an abstract type.
     */
    public static EquivalentFragmentSet isAbstract(Var start) {
        return new IsAbstractFragmentSet(start);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a type with a particular label.
     */
    public static EquivalentFragmentSet label(Var type, TypeLabel label) {
        return new LabelFragmentSet(type, label);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a resource type with a data-type.
     */
    public static EquivalentFragmentSet dataType(Var resourceType, ResourceType.DataType<?> dataType) {
        return new DataTypeFragmentSet(resourceType, dataType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a resource type whose instances must conform to a given regex.
     */
    public static EquivalentFragmentSet regex(Var resourceType, String regex) {
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

        // TODO: Create a real interface for these when there are more of them
        ImmutableList<Supplier<Boolean>> optimisations = ImmutableList.of(
                () -> applyResourceIndexOptimisation(fragmentSets, graph),
                () -> applyShortcutOptimisation(fragmentSets, graph)
        );

        // Repeatedly apply optimisations until they don't alter the query
        boolean changed = true;

        while (changed) {
            changed = false;
            for (Supplier<Boolean> optimisation : optimisations) {
                changed |= optimisation.get();
            }
        }
    }

    static <T extends EquivalentFragmentSet> Stream<T> fragmentSetOfType(
            Class<T> clazz, Collection<EquivalentFragmentSet> fragmentSets) {
        return fragmentSets.stream().filter(clazz::isInstance).map(clazz::cast);
    }

    static boolean hasDirectSubTypes(GraknGraph graph, TypeLabel label) {
        Type type = graph.getType(label);
        return type != null && type.subTypes().size() != 1;
    }
}
