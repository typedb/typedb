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
 */

package grakn.core.graql.planning.gremlin.sets;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import grakn.core.graql.planning.gremlin.fragment.Fragments;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.gremlin.EquivalentFragmentSet;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import graql.lang.statement.Variable;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.graql.planning.gremlin.sets.EquivalentFragmentSets.fragmentSetOfType;

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
public class AttributeIndexFragmentSet extends EquivalentFragmentSetImpl {

    private final Variable var;
    private final Label label;
    private final Object value;

    private AttributeIndexFragmentSet(
            Variable var,
            Label label,
            Object value) {
        super(null);
        this.var = var;
        this.label = label;
        this.value = value;
    }

    @Override
    public final Set<Fragment> fragments() {
        return ImmutableSet.of(Fragments.attributeIndex(varProperty(), var, label, value));
    }

    static final FragmentSetOptimisation ATTRIBUTE_INDEX_OPTIMISATION = (fragmentSets, conceptManager) -> {
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
                optimise(conceptManager, fragmentSets, valueSet, isaSet, label);
                return true;
            }
        }

        return false;
    };

    private static void optimise(
            ConceptManager conceptManager, Collection<EquivalentFragmentSet> fragmentSets, ValueFragmentSet valueSet, IsaFragmentSet isaSet,
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

        AttributeType.DataType<?> dataType = conceptManager.getAttributeType(label.getValue()).dataType();
        if (Number.class.isAssignableFrom(dataType.dataClass())) {
            if (dataType.dataClass() == Long.class && value instanceof Double && ((Double) value % 1 == 0)) {
                value = ((Double) value).longValue();
            } else if (dataType.dataClass() == Double.class && value instanceof Long) {
                value = ((Long) value).doubleValue();
            }
        }
        AttributeIndexFragmentSet indexFragmentSet = new AttributeIndexFragmentSet(attribute, label, value);
        fragmentSets.add(indexFragmentSet);
    }

    private static Stream<ValueFragmentSet> equalsValueFragments(Collection<EquivalentFragmentSet> fragmentSets) {
        return fragmentSetOfType(ValueFragmentSet.class, fragmentSets)
                .filter(valueFragmentSet -> valueFragmentSet.operation().isValueEquality());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof AttributeIndexFragmentSet) {
            AttributeIndexFragmentSet that = (AttributeIndexFragmentSet) o;
            return (this.var.equals(that.var))
                    && (this.label.equals(that.label))
                    && (this.value.equals(that.value));
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(var, label, value);
    }

}
