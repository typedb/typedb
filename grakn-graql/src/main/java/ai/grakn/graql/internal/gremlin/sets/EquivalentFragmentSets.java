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

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.Type;
import ai.grakn.graql.ValuePredicate;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.gremlin.sets.LabelFragmentSet.applyRedundantLabelEliminationOptimisation;
import static ai.grakn.graql.internal.gremlin.sets.ResourceIndexFragmentSet.applyResourceIndexOptimisation;
import static ai.grakn.graql.internal.gremlin.sets.ShortcutFragmentSet.applyShortcutRelationTypeOptimisation;
import static ai.grakn.graql.internal.gremlin.sets.ShortcutFragmentSet.applyShortcutRoleOptimisation;

/**
 * Factory class for producing instances of {@link EquivalentFragmentSet}.
 *
 * @author Felix Chapman
 */
public class EquivalentFragmentSets {

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a type whose instances play a role.
     *
     * @param type     a type variable label
     * @param roleType a role type variable label
     * @param required whether the plays must be constrained to be "required"
     */
    public static EquivalentFragmentSet plays(VarProperty varProperty, Var type, Var roleType, boolean required) {
        return new PlaysFragmentSet(varProperty, type, roleType, required);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a shortcut edge between two role-players.
     */
    public static EquivalentFragmentSet shortcut(VarProperty varProperty, Var relation, Var edge, Var rolePlayer, Optional<Var> roleType) {
        return new ShortcutFragmentSet(varProperty, relation, edge, rolePlayer, roleType, Optional.empty(), Optional.empty());
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a sub-type of another variable.
     */
    public static EquivalentFragmentSet sub(VarProperty varProperty, Var subType, Var superType) {
        return new SubFragmentSet(varProperty, subType, superType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a relation type which involves a role.
     */
    public static EquivalentFragmentSet relates(VarProperty varProperty, Var relationType, Var roleType) {
        return new RelatesFragmentSet(varProperty, relationType, roleType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is not a casting or a shard.
     */
    public static EquivalentFragmentSet notInternalFragmentSet(VarProperty varProperty, Var start) {
        return new NotInternalFragmentSet(varProperty, start);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is an instance of a type.
     */
    public static EquivalentFragmentSet isa(VarProperty varProperty, Var instance, Var type) {
        return new IsaFragmentSet(varProperty, instance, type);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is not equal to another variable.
     */
    public static EquivalentFragmentSet neq(VarProperty varProperty, Var varA, Var varB) {
        return new NeqFragmentSet(varProperty, varA, varB);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable represents a resource with value matching a predicate.
     */
    public static EquivalentFragmentSet value(VarProperty varProperty, Var resource, ValuePredicate predicate) {
        return new ValueFragmentSet(varProperty, resource, predicate);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a concept with a particular ID.
     */
    public static EquivalentFragmentSet id(VarProperty varProperty, Var start, ConceptId id) {
        return new IdFragmentSet(varProperty, start, id);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable represents an abstract type.
     */
    public static EquivalentFragmentSet isAbstract(VarProperty varProperty, Var start) {
        return new IsAbstractFragmentSet(varProperty, start);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a type with a particular label.
     */
    public static EquivalentFragmentSet label(VarProperty varProperty, Var type, Label label) {
        return new LabelFragmentSet(varProperty, type, label);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a resource type with a data-type.
     */
    public static EquivalentFragmentSet dataType(VarProperty varProperty, Var resourceType, AttributeType.DataType<?> dataType) {
        return new DataTypeFragmentSet(varProperty, resourceType, dataType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a resource type whose instances must conform to a given regex.
     */
    public static EquivalentFragmentSet regex(VarProperty varProperty, Var resourceType, String regex) {
        return new RegexFragmentSet(varProperty, resourceType, regex);
    }

    // TODO: Move shortcut edge optimisation here

    /**
     * Modify the given collection of {@link EquivalentFragmentSet} to introduce certain optimisations, such as the
     * {@link ResourceIndexFragmentSet}.
     * <p>
     * This involves substituting various {@link EquivalentFragmentSet} with other {@link EquivalentFragmentSet}.
     */
    public static void optimiseFragmentSets(
            Collection<EquivalentFragmentSet> fragmentSets, GraknTx graph) {

        // TODO: Create a real interface for these when there are more of them
        ImmutableList<Supplier<Boolean>> optimisations = ImmutableList.of(
                () -> applyResourceIndexOptimisation(fragmentSets, graph),
                () -> applyShortcutRoleOptimisation(fragmentSets, graph),
                () -> applyShortcutRelationTypeOptimisation(fragmentSets, graph),
                () -> applyRedundantLabelEliminationOptimisation(fragmentSets, graph)
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

    static boolean hasDirectSubTypes(GraknTx graph, Label label) {
        Type type = graph.getSchemaConcept(label);
        return type != null && !CommonUtil.containsOnly(type.subs(), 1);
    }

    static @Nullable LabelFragmentSet typeLabelOf(Var type, Collection<EquivalentFragmentSet> fragmentSets) {
        return fragmentSetOfType(LabelFragmentSet.class, fragmentSets)
                .filter(labelFragmentSet -> labelFragmentSet.type().equals(type))
                .findAny()
                .orElse(null);
    }

    @Nullable
    static IsaFragmentSet typeInformationOf(Var instance, Collection<EquivalentFragmentSet> fragmentSets) {
        return fragmentSetOfType(IsaFragmentSet.class, fragmentSets)
                .filter(isaFragmentSet -> isaFragmentSet.instance().equals(instance))
                .findAny()
                .orElse(null);
    }
}
