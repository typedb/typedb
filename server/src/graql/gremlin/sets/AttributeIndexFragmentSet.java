/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.gremlin.sets;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import grakn.core.concept.Label;
import grakn.core.concept.type.AttributeType;
import grakn.core.graql.gremlin.EquivalentFragmentSet;
import grakn.core.graql.gremlin.fragment.Fragment;
import grakn.core.graql.gremlin.fragment.Fragments;
import grakn.core.server.session.TransactionOLTP;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.graql.gremlin.sets.EquivalentFragmentSets.fragmentSetOfType;

/**
 * A query can use a more-efficient attribute index traversal when the following criteria are met:
 * <p>
 * 1. There is an IsaFragmentSet and a ValueFragmentSet referring to the same instance Variable.
 * 2. The IsaFragmentSet refers to a type Variable with a LabelFragmentSet.
 * 3. The LabelFragmentSet refers to one type in the knowledge base.
 * 4. The ValueFragmentSet is an equality predicate referring to a literal value.
 * <p>
 * When all these criteria are met, the fragments representing the IsaFragmentSet and the
 * ValueFragmentSet can be replaced with a AttributeIndexFragmentSet that will use the attribute index
 * to perform a unique lookup in constant time.
 *
 */
@AutoValue
abstract class AttributeIndexFragmentSet extends EquivalentFragmentSet {

    static AttributeIndexFragmentSet of(Variable var, Label label, Object value) {
        return new AutoValue_AttributeIndexFragmentSet(var, label, value);
    }

    @Override
    @Nullable
    public final VarProperty varProperty() {
        return null;
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(Fragments.attributeIndex(varProperty(), var(), label(), value()));
    }

    abstract Variable var();
    abstract Label label();
    abstract Object value();

    static final FragmentSetOptimisation ATTRIBUTE_INDEX_OPTIMISATION = (fragmentSets, tx) -> {
        Iterable<ValueFragmentSet> valueSets = equalsValueFragments(fragmentSets)::iterator;

        for (ValueFragmentSet valueSet : valueSets) {
            Variable attribute = valueSet.var();

            IsaFragmentSet isaSet = EquivalentFragmentSets.typeInformationOf(attribute, fragmentSets);
            if (isaSet == null) continue;

            Variable type = isaSet.type();

            LabelFragmentSet nameSet = EquivalentFragmentSets.labelOf(type, fragmentSets);
            if (nameSet == null) continue;

            Set<Label> labels = nameSet.labels();

            if (labels.size() == 1) {
                Label label = Iterables.getOnlyElement(labels);
                optimise(tx, fragmentSets, valueSet, isaSet, label);
                return true;
            }
        }

        return false;
    };

    private static void optimise(
            TransactionOLTP tx, Collection<EquivalentFragmentSet> fragmentSets, ValueFragmentSet valueSet, IsaFragmentSet isaSet,
            Label label
    ) {
        // Remove fragment sets we are going to replace
        fragmentSets.remove(valueSet);
        fragmentSets.remove(isaSet);

        // Add a new fragment set to replace the old ones
        Variable attribute = valueSet.var();

        if (!valueSet.operation().isValueEquality()) {
            throw new IllegalStateException("This optimisation should contain equalValues in equalValueFragments method");
        }

        Object value = valueSet.operation().value();

        AttributeType.DataType<?> dataType = tx.getAttributeType(label.getValue()).dataType();
        if (Number.class.isAssignableFrom(dataType.dataClass())) {
            if (dataType.dataClass() == Long.class && value instanceof Double && ((Double) value % 1 == 0)) {
                value = ((Double) value).longValue();
            } else if (dataType.dataClass() == Double.class && value instanceof Long) {
                value = ((Long) value).doubleValue();
            }
        }
        AttributeIndexFragmentSet indexFragmentSet = AttributeIndexFragmentSet.of(attribute, label, value);
        fragmentSets.add(indexFragmentSet);
    }

    private static Stream<ValueFragmentSet> equalsValueFragments(Collection<EquivalentFragmentSet> fragmentSets) {
        return fragmentSetOfType(ValueFragmentSet.class, fragmentSets)
                .filter(valueFragmentSet -> valueFragmentSet.operation().isValueEquality());
    }

}
