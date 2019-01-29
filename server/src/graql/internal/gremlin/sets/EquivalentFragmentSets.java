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

package grakn.core.graql.internal.gremlin.sets;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Relation;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.internal.executor.property.ValueExecutor;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.query.pattern.statement.Variable;
import grakn.core.graql.query.pattern.property.VarProperty;
import grakn.core.server.Transaction;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * Factory class for producing instances of {@link EquivalentFragmentSet}.
 *
 */
public class EquivalentFragmentSets {

    private static final ImmutableCollection<FragmentSetOptimisation> OPTIMISATIONS = ImmutableSet.of(
            RolePlayerFragmentSet.ROLE_OPTIMISATION,
            RolePlayerFragmentSet.IMPLICIT_RELATION_OPTIMISATION,
            AttributeIndexFragmentSet.ATTRIBUTE_INDEX_OPTIMISATION,
            RolePlayerFragmentSet.RELATION_TYPE_OPTIMISATION,
            LabelFragmentSet.REDUNDANT_LABEL_ELIMINATION_OPTIMISATION,
            SubFragmentSet.SUB_TRAVERSAL_ELIMINATION_OPTIMISATION,
            IsaFragmentSet.SKIP_EDGE_INSTANCE_CHECK_OPTIMISATION
    );

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a type whose instances play a role.
     *
     * @param type a type variable label
     * @param role a role variable label
     * @param required whether the plays must be constrained to be "required"
     */
    public static EquivalentFragmentSet plays(VarProperty varProperty, Variable type, Variable role, boolean required) {
        return new AutoValue_PlaysFragmentSet(varProperty, type, role, required);
    }

    /**
     * Describes the edge connecting a {@link Relation} to a role-player.
     * <p>
     * Can be constrained with information about the possible {@link Role}s or {@link RelationType}s.
     *
         */
    public static EquivalentFragmentSet rolePlayer(VarProperty varProperty, Variable relation, Variable edge, Variable rolePlayer, @Nullable Variable role, @Nullable ImmutableSet<Label> roleLabels, @Nullable ImmutableSet<Label> relTypeLabels) {
        return new AutoValue_RolePlayerFragmentSet(varProperty, relation, edge, rolePlayer, role, roleLabels, relTypeLabels);
    }

    public static EquivalentFragmentSet rolePlayer(VarProperty varProperty, Variable relation, Variable edge, Variable rolePlayer, @Nullable Variable role) {
        return rolePlayer(varProperty, relation, edge, rolePlayer, role, null, null);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a sub-type of another variable.
     */
    public static EquivalentFragmentSet sub(VarProperty varProperty, Variable subType, Variable superType, boolean explicitSub) {
        return new AutoValue_SubFragmentSet(varProperty, subType, superType, explicitSub);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a sub-type of another variable.
     *
     */
    public static EquivalentFragmentSet sub(VarProperty varProperty, Variable subType, Variable superType) {
        return new AutoValue_SubFragmentSet(varProperty, subType, superType, false);
    }


    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a relation type which involves a role.
     */
    public static EquivalentFragmentSet relates(VarProperty varProperty, Variable relationType, Variable role) {
        return new AutoValue_RelatesFragmentSet(varProperty, relationType, role);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is not a casting or a shard.
     */
    public static EquivalentFragmentSet notInternalFragmentSet(VarProperty varProperty, Variable start) {
        return new AutoValue_NotInternalFragmentSet(varProperty, start);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is a direct instance of a type.
     */
    public static EquivalentFragmentSet isa(
            VarProperty varProperty, Variable instance, Variable type, boolean mayHaveEdgeInstances) {
        return new AutoValue_IsaFragmentSet(varProperty, instance, type, mayHaveEdgeInstances);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable is not equal to another variable.
     */
    public static EquivalentFragmentSet neq(VarProperty varProperty, Variable varA, Variable varB) {
        return new AutoValue_NeqFragmentSet(varProperty, varA, varB);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable represents a resource with value matching a predicate.
     */
    public static EquivalentFragmentSet value(VarProperty varProperty, Variable resource, ValueExecutor.Operation<?, ?> predicate) {
        return new ValueFragmentSet(varProperty, resource, predicate);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a concept with a particular ID.
     */
    public static EquivalentFragmentSet id(VarProperty varProperty, Variable start, ConceptId id) {
        return new AutoValue_IdFragmentSet(varProperty, start, id);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable represents an abstract type.
     */
    public static EquivalentFragmentSet isAbstract(VarProperty varProperty, Variable start) {
        return new AutoValue_IsAbstractFragmentSet(varProperty, start);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a schema concept with one of the
     * specified labels.
     */
    public static EquivalentFragmentSet label(VarProperty varProperty, Variable type, ImmutableSet<Label> labels) {
        return new AutoValue_LabelFragmentSet(varProperty, type, labels);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a variable representing a resource type with a data-type.
     */
    public static EquivalentFragmentSet dataType(VarProperty varProperty, Variable resourceType, AttributeType.DataType<?> dataType) {
        return new AutoValue_DataTypeFragmentSet(varProperty, resourceType, dataType);
    }

    /**
     * An {@link EquivalentFragmentSet} that indicates a resource type whose instances must conform to a given regex.
     */
    public static EquivalentFragmentSet regex(VarProperty varProperty, Variable resourceType, String regex) {
        return new AutoValue_RegexFragmentSet(varProperty, resourceType, regex);
    }

    /**
     * Modify the given collection of {@link EquivalentFragmentSet} to introduce certain optimisations, such as the
     * {@link AttributeIndexFragmentSet}.
     * <p>
     * This involves substituting various {@link EquivalentFragmentSet} with other {@link EquivalentFragmentSet}.
     */
    public static void optimiseFragmentSets(
            Collection<EquivalentFragmentSet> fragmentSets, Transaction tx) {

        // Repeatedly apply optimisations until they don't alter the query
        boolean changed = true;

        while (changed) {
            changed = false;
            for (FragmentSetOptimisation optimisation : OPTIMISATIONS) {
                changed |= optimisation.apply(fragmentSets, tx);
            }
        }
    }

    static <T extends EquivalentFragmentSet> Stream<T> fragmentSetOfType(
            Class<T> clazz, Collection<EquivalentFragmentSet> fragmentSets) {
        return fragmentSets.stream().filter(clazz::isInstance).map(clazz::cast);
    }

    static @Nullable LabelFragmentSet labelOf(Variable type, Collection<EquivalentFragmentSet> fragmentSets) {
        return fragmentSetOfType(LabelFragmentSet.class, fragmentSets)
                .filter(labelFragmentSet -> labelFragmentSet.var().equals(type))
                .findAny()
                .orElse(null);
    }

    @Nullable
    static IsaFragmentSet typeInformationOf(Variable instance, Collection<EquivalentFragmentSet> fragmentSets) {
        return fragmentSetOfType(IsaFragmentSet.class, fragmentSets)
                .filter(isaFragmentSet -> isaFragmentSet.instance().equals(instance))
                .findAny()
                .orElse(null);
    }
}
